package com.alibaba.grape.annotation;

import com.alibaba.fastffi.*;
import com.alibaba.fastffi.annotation.AnnotationProcessorUtils;
import com.alibaba.grape.ds.AdjList;
import com.alibaba.grape.ds.Nbr;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import com.alibaba.grape.parallel.MessageInBuffer;
import com.alibaba.grape.parallel.ParallelMessageManager;
import com.alibaba.grape.utils.CppClassName;
import com.squareup.javapoet.*;
import io.graphscope.ds.*;
import io.graphscope.fragment.ArrowFragment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.*;

import static io.graphscope.utils.CppClassName.ARROW_FRAGMENT;

@SupportedAnnotationTypes({"com.alibaba.fastffi.FFIMirror", "com.alibaba.fastffi.FFIMirrorDefinition",
        "com.alibaba.grape.annotation.GraphType"})
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class GrapeAnnotationProcessor extends javax.annotation.processing.AbstractProcessor {
    public static final String GraphTypeWrapperSuffix = "$$GraphTypeDefinitions";
    static final String JNI_HEADER = "<jni.h>";
    private static Logger logger = LoggerFactory.getLogger(GrapeAnnotationProcessor.class.getName());
    Map<String, String> foreignTypeNameToJavaTypeName = new HashMap<>();
    Map<String, FFIMirrorDefinition> foreignTypeNameToFFIMirrorDefinitionMap = new HashMap<>();
    Map<String, FFIMirrorDefinition> javaTypeNameToFFIMirrorDefinitionMap = new HashMap<>();
    String graphTypeElementName;
    boolean graphTypeWrapperGenerated = false;
    AnnotationMirror graphType = null;
    private String javaImmutableFragmentTemplateName = ImmutableEdgecutFragment.class.getName();
    private String foreignImmutableFragmentTemplateName = CppClassName.GRAPE_IMMUTABLE_FRAGMENT;

    private String JavaArrowFragmentTemplateName = ArrowFragment.class.getName();
    private String foreignArrowFragmentTemplateName = ARROW_FRAGMENT;

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (roundEnv.processingOver()) {
            output();
            return false;
        }
        for (TypeElement annotation : annotations) {
            for (Element e : roundEnv.getElementsAnnotatedWith(annotation)) {
                if (e instanceof TypeElement) {
                    processAnnotation((TypeElement) e, annotation);
                }
            }
        }
        return false;
    }

    public List<String> getMessageTypes() {
        List<String> messageTypes = new ArrayList<>();
        String messageTypeString = System.getProperty("grape.messageTypes");
        if (messageTypeString != null) {
            Arrays.asList(parseMessageTypes(messageTypeString)).forEach(p -> messageTypes.add(p));
        }
        return messageTypes;
    }

    /**
     * Use : to separate types
     *
     * @param messageTypes
     * @return
     */
    private String[] parseMessageTypes(String messageTypes) {
        String[] results =
                Arrays.stream(messageTypes.split(",")).map(m -> m.trim()).toArray(String[]::new);
        return results;
    }

    /**
     * Given one FFIMirror class name, we need to get the foreign type too.
     * Currently don't support primitive types
     * if message is ffi, we use the foreign type.
     *
     * @param messageType
     * @return String [0] cxx type, String [1] java type
     */
    public String[] parseMessageType(String messageType) {
//    String[] results = new String [2];
//    results[1] = messageType;
//    Class<?> res = null;
//    try {
//      res = Class.forName(messageType);
//    }
//    catch (ClassNotFoundException e){
//      logger.error("Message class " + messageType + " not found.");
//      e.printStackTrace();
//    }
//    results[0] = FFITypeFactory.getFFITypeName(res,true);
        return messageType.split("=");
    }

    boolean isGraphType(AnnotationMirror annotationMirror) {
        DeclaredType declaredType = annotationMirror.getAnnotationType();
        TypeMirror graphTypeAnnotationType =
                processingEnv.getElementUtils().getTypeElement(GraphType.class.getName()).asType();
        return processingEnv.getTypeUtils().isSameType(declaredType, graphTypeAnnotationType);
    }

    void assertNull(Object obj) {
        if (obj != null) {
            throw new IllegalArgumentException("Require null, got " + obj);
        }
    }

    void assertNullOrEquals(Object obj, Object expected) {
        if (obj != null && !obj.equals(expected)) {
            throw new IllegalArgumentException("Require null or " + expected + ", got " + obj);
        }
    }

    void processAnnotation(TypeElement typeElement, TypeElement annotation) {
        if (isSameType(annotation.asType(), FFIMirrorDefinition.class)) {
            FFIMirrorDefinition mirrorDefinition = typeElement.getAnnotation(FFIMirrorDefinition.class);
            if (mirrorDefinition != null) {
                String foreignType = getFFIMirrorForeignTypeName(mirrorDefinition);
                String javaType = getFFIMirrorJavaTypeName(typeElement);
                assertNull(foreignTypeNameToFFIMirrorDefinitionMap.put(foreignType, mirrorDefinition));
                assertNull(javaTypeNameToFFIMirrorDefinitionMap.put(javaType, mirrorDefinition));
                assertNullOrEquals(foreignTypeNameToJavaTypeName.put(foreignType, javaType), javaType);
                checkAndGenerateGraphTypeWrapper();
            }
            return;
        }

        if (isSameType(annotation.asType(), GraphType.class)) {
            List<? extends AnnotationMirror> mirrors = typeElement.getAnnotationMirrors();
            for (AnnotationMirror annotationMirror : mirrors) {
                if (isGraphType(annotationMirror)) {
                    if (this.graphType != null) {
                        throw new IllegalStateException("Oops: An project can only have one @GraphType, "
                                + "already have " + this.graphType + ", but got "
                                + annotationMirror);
                    }
                    this.graphType = annotationMirror;
                    this.graphTypeElementName = typeElement.getQualifiedName().toString();
                    checkAndGenerateGraphTypeWrapper();
                }
            }
            return;
        }

        if (isSameType(annotation.asType(), FFIMirror.class)) {
            String foreignType = getForeignTypeNameByFFITypeAlias(typeElement);
            String javaType = typeElement.getQualifiedName().toString();
            String check = foreignTypeNameToJavaTypeName.put(foreignType, javaType);
            if (check != null) {
                processingEnv.getMessager().printMessage(
                        Diagnostic.Kind.ERROR,
                        "Duplicalte FFIMirror on " + foreignType + ", expected " + check + ", got " + javaType);
            }
            return;
        }
    }

    /**
     * @param typeElement: the generated implementation of the FFIMirror
     * @return
     */
    private String getFFIMirrorJavaTypeName(TypeElement typeElement) {
        List<? extends TypeMirror> interfaces = typeElement.getInterfaces();
        if (interfaces.size() != 1) {
            throw new IllegalArgumentException("A generated class must only have one super interface: "
                    + typeElement);
        }
        return AnnotationProcessorUtils.typeToTypeName(interfaces.get(0));
    }

    /**
     * Return the package element of the type element.
     *
     * @param typeElement
     * @return
     */
    private PackageElement getPackageElement(TypeElement typeElement) {
        return AnnotationProcessorUtils.getPackageElement(typeElement);
    }

    private void checkAndGenerateGraphTypeWrapper() {
        if (graphTypeWrapperGenerated == true) {
            return;
        }
        if (!checkPrecondition()) {
            return;
        }
        generateGraphTypeWrapper();
    }

    private void addGrapeWrapper(TypeSpec.Builder classBuilder) {
        DeclaredType edataType = getEdataType();
        String foreignEdataType = getForeignTypeName(edataType);
        String javaEdataType = getTypeName(edataType);
        DeclaredType oidType = getOidType();
        String foreignOidType = getForeignTypeName(oidType);
        String javaOidType = getTypeName(oidType);
        DeclaredType vdataType = getVdataType();
        String foreignVdataType = getForeignTypeName(vdataType);
        String javaVdataType = getTypeName(vdataType);
        // String foreignImmutableFragmentTemplateName = "grape::JavaImmutableEdgecutFragment";
        String foreignImmutableFragNameConcat = makeParameterizedType(foreignImmutableFragmentTemplateName, foreignOidType, "uint64_t", foreignVdataType, foreignEdataType);
        String javaImmutableFragNameConcat = makeParameterizedType(javaImmutableFragmentTemplateName, javaOidType, "java.lang.Long", javaVdataType, javaEdataType);

        AnnotationSpec.Builder ffiGenBatchBuilder = AnnotationSpec.builder(FFIGenBatch.class);
        ffiGenBatchBuilder
                .addMember("value", "$L",
                        AnnotationSpec.builder(FFIGen.class)
                                .addMember("type", "$S", AdjList.class.getName())
                                .addMember("templates", "$L",
                                        AnnotationSpec.builder(CXXTemplate.class)
                                                .addMember("cxx", "$S", "uint32_t")
                                                .addMember("cxx", "$S", foreignEdataType)
                                                .addMember("java", "$S", Integer.class.getName())
                                                .addMember("java", "$S", javaEdataType)
                                                .build())
                                .addMember("templates", "$L",
                                        AnnotationSpec.builder(CXXTemplate.class)
                                                .addMember("cxx", "$S", "uint64_t")
                                                .addMember("cxx", "$S", foreignEdataType)
                                                .addMember("java", "$S", Long.class.getName())
                                                .addMember("java", "$S", javaEdataType)
                                                .build())
                                .build())
                .addMember("value", "$L",
                        AnnotationSpec.builder(FFIGen.class)
                                .addMember("type", "$S", Nbr.class.getName())
                                .addMember("templates", "$L",
                                        AnnotationSpec.builder(CXXTemplate.class)
                                                .addMember("cxx", "$S", "uint32_t")
                                                .addMember("cxx", "$S", foreignEdataType)
                                                .addMember("java", "$S", Integer.class.getName())
                                                .addMember("java", "$S", javaEdataType)
                                                .build())
                                .addMember("templates", "$L",
                                        AnnotationSpec.builder(CXXTemplate.class)
                                                .addMember("cxx", "$S", "uint64_t")
                                                .addMember("cxx", "$S", foreignEdataType)
                                                .addMember("java", "$S", Long.class.getName())
                                                .addMember("java", "$S", javaEdataType)
                                                .build())
                                .build())
                .addMember(
                        "value", "$L",
                        AnnotationSpec.builder(FFIGen.class)
                                .addMember("type", "$S", javaImmutableFragmentTemplateName)
                                .addMember("templates", "$L",
                                        AnnotationSpec.builder(CXXTemplate.class)
                                                .addMember("cxx", "$S", foreignOidType)
                                                .addMember("cxx", "$S", "uint32_t")
                                                .addMember("cxx", "$S", foreignVdataType)
                                                .addMember("cxx", "$S", foreignEdataType)
                                                .addMember("java", "$S", javaOidType)
                                                .addMember("java", "$S", Integer.class.getName())
                                                .addMember("java", "$S", javaVdataType)
                                                .addMember("java", "$S", javaEdataType)
//                                                .addMember("include", "$L",
//                                                        AnnotationSpec.builder(CXXHead.class)
//                                                                .addMember("value", "$S", "grape_gen_def.h")
//                                                                .build())
                                                .build())
                                .addMember("templates", "$L",
                                        AnnotationSpec.builder(CXXTemplate.class)
                                                .addMember("cxx", "$S", foreignOidType)
                                                .addMember("cxx", "$S", "uint64_t")
                                                .addMember("cxx", "$S", foreignVdataType)
                                                .addMember("cxx", "$S", foreignEdataType)
                                                .addMember("java", "$S", javaOidType)
                                                .addMember("java", "$S", Long.class.getName())
                                                .addMember("java", "$S", javaVdataType)
                                                .addMember("java", "$S", javaEdataType)
                                                .build())
                                .build());

        {
            AnnotationSpec.Builder ffiGenBuilderdefault = AnnotationSpec.builder(FFIGen.class);
            ffiGenBuilderdefault
                    .addMember("type", "$S",
                            DefaultMessageManager.class.getName());
            AnnotationSpec.Builder ffiGenBuilderParallel = AnnotationSpec.builder(FFIGen.class);
            ffiGenBuilderParallel
                    .addMember("type", "$S",
                            ParallelMessageManager.class.getName());
            AnnotationSpec.Builder ffiGenBuilderMsgInBuffer = AnnotationSpec.builder(FFIGen.class);
            ffiGenBuilderMsgInBuffer
                    .addMember("type", "$S",
                            MessageInBuffer.class.getName());
            addMessageTypesDefault(ffiGenBuilderdefault, foreignImmutableFragNameConcat, javaImmutableFragNameConcat);
            addMessageTypesParallel(ffiGenBuilderParallel, foreignImmutableFragNameConcat, javaImmutableFragNameConcat);
            addMessageTypesMsgInBuffer(ffiGenBuilderMsgInBuffer, foreignImmutableFragNameConcat, javaImmutableFragNameConcat);
            ffiGenBatchBuilder.addMember("value", "$L", ffiGenBuilderdefault.build());
            ffiGenBatchBuilder.addMember("value", "$L", ffiGenBuilderParallel.build());
            ffiGenBatchBuilder.addMember("value", "$L", ffiGenBuilderMsgInBuffer.build());
        }
        {
            //work for arrowFragment
            AnnotationSpec.Builder ffiGenBuilderArrowFragDefault = AnnotationSpec.builder(FFIGen.class);
            ffiGenBuilderArrowFragDefault
                    .addMember("type", "$S",
                            JavaArrowFragmentTemplateName)
                    .addMember("templates", "$L",
                            AnnotationSpec.builder(CXXTemplate.class)
                                    .addMember("cxx", "$S", "int64_t")
                                    .addMember("java", "$S", Long.class.getName())
//                                    .addMember("include", "$L",
//                                            AnnotationSpec.builder(CXXHead.class)
//                                                    .addMember("value", "$S", JAVA_APP_JNI_FFI_H)
//                                                    .addMember("value", "$S", "grape-gen.h")
//                                                    .build())
                                    .build());
            ffiGenBatchBuilder.addMember("value", "$L", ffiGenBuilderArrowFragDefault.build());
        }
        {
            //PropertyAdjList
            AnnotationSpec.Builder ffiGenBuilderPropertyAdjlist = AnnotationSpec.builder(FFIGen.class);
            ffiGenBuilderPropertyAdjlist
                    .addMember("type", "$S", PropertyAdjList.class.getName())
                    .addMember("templates", "$L",
                            AnnotationSpec.builder(CXXTemplate.class)
                                    .addMember("cxx", "$S", "uint64_t")
                                    .addMember("java", "$S", Long.class.getName())
                                    .build()
                    );
            ffiGenBatchBuilder.addMember("value", "$L", ffiGenBuilderPropertyAdjlist.build());
        }
        {
            //PropertyRawAdjList
            AnnotationSpec.Builder ffiGenBuildPropertyRawAdjList = AnnotationSpec.builder(FFIGen.class);
            ffiGenBuildPropertyRawAdjList
                    .addMember("type", "$S", PropertyRawAdjList.class.getName())
                    .addMember("templates", "$L",
                            AnnotationSpec.builder(CXXTemplate.class)
                                    .addMember("cxx", "$S", "uint64_t")
                                    .addMember("java", "$S", Long.class.getName())
                                    .build()
                    );
            ffiGenBatchBuilder.addMember("value", "$L", ffiGenBuildPropertyRawAdjList.build());
        }
        {
            //EdgeDataColumn
            AnnotationSpec.Builder ffiGenBuildEdgeDataColumn = AnnotationSpec.builder(FFIGen.class);
            ffiGenBuildEdgeDataColumn
                    .addMember("type", "$S", EdgeDataColumn.class.getName())
                    .addMember("templates", "$L",
                            AnnotationSpec.builder(CXXTemplate.class)
                                    .addMember("cxx", "$S", "int64_t")
                                    .addMember("java", "$S", Long.class.getName())
                                    .build())
                    .addMember("templates", "$L",
                            AnnotationSpec.builder(CXXTemplate.class)
                                    .addMember("cxx", "$S", "double") //data_t
                                    .addMember("java", "$S", Double.class.getName())
                                    .build())
                    .addMember("templates", "$L",
                            AnnotationSpec.builder(CXXTemplate.class)
                                    .addMember("cxx", "$S", "int32_t") //data_t
                                    .addMember("java", "$S", Integer.class.getName())
                                    .build());
            ffiGenBatchBuilder.addMember("value", "$L", ffiGenBuildEdgeDataColumn.build());
        }
        {
            //vertexDataColumn
            AnnotationSpec.Builder ffiGenBuildVertexDataColumn = AnnotationSpec.builder(FFIGen.class);
            ffiGenBuildVertexDataColumn
                    .addMember("type", "$S", VertexDataColumn.class.getName())
                    .addMember("templates", "$L",
                            AnnotationSpec.builder(CXXTemplate.class)
                                    .addMember("cxx", "$S", "int64_t")
                                    .addMember("java", "$S", Long.class.getName())
                                    .build())
                    .addMember("templates", "$L",
                            AnnotationSpec.builder(CXXTemplate.class)
                                    .addMember("cxx", "$S", "double") //data_t
                                    .addMember("java", "$S", Double.class.getName())
                                    .build())
                    .addMember("templates", "$L",
                            AnnotationSpec.builder(CXXTemplate.class)
                                    .addMember("cxx", "$S", "int32_t") //data_t
                                    .addMember("java", "$S", Integer.class.getName())
                                    .build());
            ffiGenBatchBuilder.addMember("value", "$L", ffiGenBuildVertexDataColumn.build());
        }
        {
            //PropertyNbr
            AnnotationSpec.Builder ffiGenBuildPropertyNbr = AnnotationSpec.builder(FFIGen.class);
            ffiGenBuildPropertyNbr
                    .addMember("type", "$S", PropertyNbr.class.getName())
                    .addMember("templates", "$L",
                            AnnotationSpec.builder(CXXTemplate.class)
                                    .addMember("cxx", "$S", "uint64_t")
                                    .addMember("java", "$S", Long.class.getName())
                                    .build()
                    );
            ffiGenBatchBuilder.addMember("value", "$L", ffiGenBuildPropertyNbr.build());
        }
        {
            //PropertyNbrUnit
            AnnotationSpec.Builder ffiGenBuildPropertyNbrUnit = AnnotationSpec.builder(FFIGen.class);
            ffiGenBuildPropertyNbrUnit
                    .addMember("type", "$S", PropertyNbrUnit.class.getName())
                    .addMember("templates", "$L",
                            AnnotationSpec.builder(CXXTemplate.class)
                                    .addMember("cxx", "$S", "uint64_t")
                                    .addMember("java", "$S", Long.class.getName())
                                    .build()
                    );
            ffiGenBatchBuilder.addMember("value", "$L", ffiGenBuildPropertyNbrUnit.build());
        }
//        {
//            //PropertyMessageManager
//            AnnotationSpec.Builder ffiGenBuildPropertyMM = AnnotationSpec.builder(FFIGen.class);
//            ffiGenBuildPropertyMM
//                    .addMember("type", "$S", PropertyMessageManager.class.getName());
//            propertyMMAddMessages(ffiGenBuildPropertyMM);
//            ffiGenBatchBuilder.addMember("value", "$L", ffiGenBuildPropertyMM.build());
//        }

        classBuilder.addAnnotation(ffiGenBatchBuilder.build());
    }

    private void addMessageTypesDefault(AnnotationSpec.Builder ffiGenBuilder, String foreignImmutableFragNameConcat, String javaImmutableFragNameConcat) {
        List<String> messageTypes = getMessageTypes();
        logger.info("In default mm, received message types are: " + String.join(",", messageTypes));
        if (messageTypes.isEmpty()) {
            return;
        }
        {
            // getMsg
            AnnotationSpec.Builder ffiFunGenBuilder = AnnotationSpec.builder(FFIFunGen.class)
                    .addMember("name", "$S", "getMessage")
                    .addMember("returnType", "$S", "boolean")
                    .addMember("parameterTypes", "$S", "FRAG_T")
                    .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignImmutableFragNameConcat)
                                .addMember("cxx", "$S", types[0])
                                .addMember("java", "$S", javaImmutableFragNameConcat)
                                .addMember("java", "$S", types[1]);
                ffiFunGenBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            ffiGenBuilder.addMember("functionTemplates", "$L", ffiFunGenBuilder.build());
        }
        {
            // SyncStateOnOuterVertex
            AnnotationSpec.Builder ffiFunGenBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "syncStateOnOuterVertex")
                            .addMember("returnType", "$S", "void")
                            .addMember("parameterTypes", "$S", "FRAG_T")
                            .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignImmutableFragNameConcat)
                                .addMember("cxx", "$S", types[0])
                                .addMember("java", "$S", javaImmutableFragNameConcat)
                                .addMember("java", "$S", types[1]);
                ffiFunGenBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            ffiGenBuilder.addMember("functionTemplates", "$L", ffiFunGenBuilder.build());
        }
        {
            // sendMsgThroughIEdges
            AnnotationSpec.Builder ffiFunGenBuilder = AnnotationSpec.builder(FFIFunGen.class)
                    .addMember("name", "$S", "sendMsgThroughIEdges")
                    .addMember("returnType", "$S", "void")
                    .addMember("parameterTypes", "$S", "FRAG_T")
                    .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignImmutableFragNameConcat)
                                .addMember("cxx", "$S", types[0])
                                .addMember("java", "$S", javaImmutableFragNameConcat)
                                .addMember("java", "$S", types[1]);
                ffiFunGenBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            ffiGenBuilder.addMember("functionTemplates", "$L", ffiFunGenBuilder.build());
        }
        {
            // sendMsgThroughOEdges
            AnnotationSpec.Builder ffiFunGenBuilder = AnnotationSpec.builder(FFIFunGen.class)
                    .addMember("name", "$S", "sendMsgThroughOEdges")
                    .addMember("returnType", "$S", "void")
                    .addMember("parameterTypes", "$S", "FRAG_T")
                    .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignImmutableFragNameConcat)
                                .addMember("cxx", "$S", types[0])
                                .addMember("java", "$S", javaImmutableFragNameConcat)
                                .addMember("java", "$S", types[1]);
                ffiFunGenBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            ffiGenBuilder.addMember("functionTemplates", "$L", ffiFunGenBuilder.build());
        }
        {
            // sendMsgThroughEdges
            AnnotationSpec.Builder ffiFunGenBuilder = AnnotationSpec.builder(FFIFunGen.class)
                    .addMember("name", "$S", "sendMsgThroughEdges")
                    .addMember("returnType", "$S", "void")
                    .addMember("parameterTypes", "$S", "FRAG_T")
                    .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignImmutableFragNameConcat)
                                .addMember("cxx", "$S", types[0])
                                .addMember("java", "$S", javaImmutableFragNameConcat)
                                .addMember("java", "$S", types[1]);
                ffiFunGenBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            ffiGenBuilder.addMember("functionTemplates", "$L", ffiFunGenBuilder.build());
        }
    }

    private void addMessageTypesParallel(AnnotationSpec.Builder ffiGenBuilder, String foreignImmutableFragNameConcat, String javaImmutableFragNameConcat) {
        List<String> messageTypes = getMessageTypes();
        logger.info("In parallel mm, received message types are: " + String.join(",", messageTypes));
        if (messageTypes.isEmpty()) {
            return;
        }
        { // SyncStateOnOuterVertex
            AnnotationSpec.Builder ffiFunGenBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "syncStateOnOuterVertex")
                            .addMember("returnType", "$S", "void")
                            .addMember("parameterTypes", "$S", "FRAG_T")
                            .addMember("parameterTypes", "$S", "MSG_T")
                            .addMember("parameterTypes", "$S", "int");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignImmutableFragNameConcat)
                                .addMember("cxx", "$S", types[0])
                                .addMember("java", "$S", javaImmutableFragNameConcat)
                                .addMember("java", "$S", types[1]);
                ffiFunGenBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            ffiGenBuilder.addMember("functionTemplates", "$L", ffiFunGenBuilder.build());
        }

        { // SyncStateOnOuterVertex overloaded for no msg
            AnnotationSpec.Builder ffiFunGenBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "syncStateOnOuterVertex")
                            .addMember("returnType", "$S", "void")
                            .addMember("parameterTypes", "$S", "FRAG_T")
                            .addMember("parameterTypes", "$S", "int");
            AnnotationSpec.Builder templateBuilder =
                    AnnotationSpec.builder(CXXTemplate.class)
                            .addMember("cxx", "$S", foreignImmutableFragNameConcat)
                            .addMember("java", "$S", javaImmutableFragNameConcat);
            ffiFunGenBuilder.addMember("templates", "$L", templateBuilder.build());
            ffiGenBuilder.addMember("functionTemplates", "$L", ffiFunGenBuilder.build());
        }

        {//sendMsgThroughOEdges
            AnnotationSpec.Builder ffiFunGenBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "sendMsgThroughOEdges")
                            .addMember("returnType", "$S", "void")
                            .addMember("parameterTypes", "$S", "FRAG_T")
                            .addMember("parameterTypes", "$S", "MSG_T")
                            .addMember("parameterTypes", "$S", "int");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignImmutableFragNameConcat)
                                .addMember("cxx", "$S", types[0])
                                .addMember("java", "$S", javaImmutableFragNameConcat)
                                .addMember("java", "$S", types[1]);
                ffiFunGenBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            ffiGenBuilder.addMember("functionTemplates", "$L", ffiFunGenBuilder.build());
        }

        {//sendMsgThroughIEdges
            AnnotationSpec.Builder ffiFunGenBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "sendMsgThroughIEdges")
                            .addMember("returnType", "$S", "void")
                            .addMember("parameterTypes", "$S", "FRAG_T")
                            .addMember("parameterTypes", "$S", "MSG_T")
                            .addMember("parameterTypes", "$S", "int");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignImmutableFragNameConcat)
                                .addMember("cxx", "$S", types[0])
                                .addMember("java", "$S", javaImmutableFragNameConcat)
                                .addMember("java", "$S", types[1]);
                ffiFunGenBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            ffiGenBuilder.addMember("functionTemplates", "$L", ffiFunGenBuilder.build());
        }
        {//sendMsgThroughEdges
            AnnotationSpec.Builder ffiFunGenBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "sendMsgThroughEdges")
                            .addMember("returnType", "$S", "void")
                            .addMember("parameterTypes", "$S", "VERTEX_T")
                            .addMember("parameterTypes", "$S", "MSG_T")
                            .addMember("parameterTypes", "$S", "int");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignImmutableFragNameConcat)
                                .addMember("cxx", "$S", types[0])
                                .addMember("java", "$S", javaImmutableFragNameConcat)
                                .addMember("java", "$S", types[1]);
                ffiFunGenBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            ffiGenBuilder.addMember("functionTemplates", "$L", ffiFunGenBuilder.build());
        }

//    { // GetMessageInBuffer
//      AnnotationSpec.Builder ffiFunGenBuilder =
//          AnnotationSpec.builder(FFIFunGen.class)
//              .addMember("name", "$S", "GetMessageInBuffer")
//              .addMember("returnType", "$S", "boolean")
//              .addMember("parameterTypes", "$S", "MSG_BUFFER_T");
//
//      String foreignJavaparallelMessageManager = "grape::JavaParallelMessageManager";
//      String javaJavaparallelMessageManager =
//          "com.alibaba.grape.message.messageManager.JavaParallelMessageManager";
//      DeclaredType oidType = getOidType();
//      DeclaredType edataType = getEdataType();
//      DeclaredType vdataType = getVdataType();
//      AnnotationSpec.Builder templateBuilder =
//          AnnotationSpec.builder(CXXTemplate.class)
//              .addMember("cxx", "$S",
//                         makeParameterizedType(
//                             foreignJavaparallelMessageManager,
//                             makeParameterizedType(foreignImmutableFragmentTemplateName,
//                                                   getForeignTypeName(oidType), "uint64_t",
//                                                   getForeignTypeName(vdataType),
//                                                   getForeignTypeName(edataType))))
//              .addMember(
//                  "java", "$S",
//                  makeParameterizedType(
//                      javaJavaparallelMessageManager,
//                      makeParameterizedType(javaImmutableFragmentTemplateName, getTypeName(oidType),
//                                            Long.class.getName(), getTypeName(vdataType),
//                                            getTypeName(edataType))));
//      ffiFunGenBuilder.addMember("templates", "$L", templateBuilder.build());
//      ffiGenBuilder.addMember("functionTemplates", "$L", ffiFunGenBuilder.build());
//    }
    }

    private void addMessageTypesMsgInBuffer(AnnotationSpec.Builder ffiGenBuilder, String foreignImmutableFragNameConcat, String javaImmutableFragNameConcat) {
        List<String> messageTypes = getMessageTypes();
        logger.info("In messageInBuffer, received message types are: " + String.join(",", messageTypes));
        if (messageTypes.isEmpty()) {
            return;
        }
        { // getMessage
            AnnotationSpec.Builder ffiFunGenBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "getMessage")
                            .addMember("returnType", "$S", "boolean")
                            .addMember("parameterTypes", "$S", "FRAG_T")
                            .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignImmutableFragNameConcat)
                                .addMember("cxx", "$S", types[0])
                                .addMember("java", "$S", javaImmutableFragNameConcat)
                                .addMember("java", "$S", types[1]);
                ffiFunGenBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            ffiGenBuilder.addMember("functionTemplates", "$L", ffiFunGenBuilder.build());
        }
    }

    private void propertyMMAddMessages(AnnotationSpec.Builder propertyMMBuilder) {
        List<String> messageTypes = getMessageTypes();
        logger.info("In propertyMessageManager, received message types are: " + String.join(",", messageTypes));
        if (messageTypes.isEmpty()) {
            return;
        }
        {
            //sendMsgThroughIEdges
            AnnotationSpec.Builder sendIEdgesBuilder = AnnotationSpec.builder(FFIFunGen.class)
                    .addMember("name", "$S", "sendMsgThroughIEdges")
                    .addMember("returnType", "$S", "void")
                    .addMember("parameterTypes", "$S", "FRAG_T")
//                    .addMember("parameterTypes", "$S", "VERTEX_T")
//                    .addMember("parameterTypes", "$S", "LABEL_ID_T")
                    .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", makeParameterizedType(ARROW_FRAGMENT, "int64_t"))
                                .addMember("cxx", "$S", types[0])
                                .addMember(
                                        "java", "$S",
                                        makeParameterizedType(JavaArrowFragmentTemplateName, Long.class.getName()))
                                .addMember("java", "$S", types[1]);
                sendIEdgesBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            propertyMMBuilder.addMember("functionTemplates", "$L", sendIEdgesBuilder.build());
        }
        {
            //sendMsgThroughOEdges
            AnnotationSpec.Builder sendOEdgesBuilder = AnnotationSpec.builder(FFIFunGen.class)
                    .addMember("name", "$S", "sendMsgThroughOEdges")
                    .addMember("returnType", "$S", "void")
                    .addMember("parameterTypes", "$S", "FRAG_T")
                    .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", makeParameterizedType(ARROW_FRAGMENT, "int64_t"))
                                .addMember("cxx", "$S", types[0])
                                .addMember(
                                        "java", "$S",
                                        makeParameterizedType(JavaArrowFragmentTemplateName, Long.class.getName()))
                                .addMember("java", "$S", types[1]);
                sendOEdgesBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            propertyMMBuilder.addMember("functionTemplates", "$L", sendOEdgesBuilder.build());
        }
        {
            //sendMsgThroughEdges
            AnnotationSpec.Builder sendEdgesBuilder = AnnotationSpec.builder(FFIFunGen.class)
                    .addMember("name", "$S", "sendMsgThroughEdges")
                    .addMember("returnType", "$S", "void")
                    .addMember("parameterTypes", "$S", "FRAG_T")
                    .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", makeParameterizedType(ARROW_FRAGMENT, "int64_t"))
                                .addMember("cxx", "$S", types[0])
                                .addMember(
                                        "java", "$S",
                                        makeParameterizedType(JavaArrowFragmentTemplateName, Long.class.getName()))
                                .addMember("java", "$S", types[1]);
                sendEdgesBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            propertyMMBuilder.addMember("functionTemplates", "$L", sendEdgesBuilder.build());
        }
    }

    private String makeParameterizedType(String base, String... types) {
        if (types.length == 0) {
            return base;
        }
        return base + "<" + String.join(",", types) + ">";
    }

    private void generateGrapeWrapper() {
        Random random = new Random();
        String packageName = "grape" + random.nextInt(Integer.MAX_VALUE);
        String classSimpleName = "GrapeWrapper" + random.nextInt(Integer.MAX_VALUE);
        TypeSpec.Builder classBuilder =
                TypeSpec.interfaceBuilder(classSimpleName).addModifiers(Modifier.PUBLIC);
        addGrapeWrapper(classBuilder);
        writeTypeSpec(packageName, classBuilder.build());
    }

    private void generateGraphTypeWrapper() {
        TypeElement typeElement = processingEnv.getElementUtils().getTypeElement(graphTypeElementName);
        String name = typeElement.getSimpleName().toString() + GraphTypeWrapperSuffix;
        String libraryName = null;
        PackageElement packageElement = getPackageElement(typeElement);
        FFIApplication application = packageElement.getAnnotation(FFIApplication.class);
        if (application != null) {
            libraryName = application.jniLibrary();
            // throw new IllegalStateException("The class annotated by @GraphType must be in package with
            // a package-info.java and the package info must be annotated with FFIApplication.");
        }

        TypeSpec.Builder classBuilder = TypeSpec.interfaceBuilder(name).addModifiers(Modifier.PUBLIC);
        classBuilder.addSuperinterface(getTypeMirror(FFIPointer.class.getName()));
        if (libraryName == null || libraryName.isEmpty()) {
            classBuilder.addAnnotation(FFIGen.class);
        } else {
            classBuilder.addAnnotation(
                    AnnotationSpec.builder(FFIGen.class).addMember("value", "$S", libraryName).build());
        }
        classBuilder.addAnnotation(FFIMirror.class);
        classBuilder.addAnnotation(
                AnnotationSpec.builder(FFITypeAlias.class).addMember("value", "$S", name).build());

        Set<String> headers = getGraphTypeHeaders();
        for (String header : headers) {
            if (header.length() <= 2) {
                throw new IllegalStateException("Invalid header: " + header);
            }
            String internal = header.substring(1, header.length() - 1);
            if (header.charAt(0) == '<') {
                classBuilder.addAnnotation(
                        AnnotationSpec.builder(CXXHead.class).addMember("system", "$S", internal).build());
            } else {
                classBuilder.addAnnotation(
                        AnnotationSpec.builder(CXXHead.class).addMember("value", "$S", internal).build());
            }
        }

        addGraphTypeWrapper(classBuilder, "vidType", getVidType());
        addGraphTypeWrapper(classBuilder, "edataType", getEdataType());
        addGraphTypeWrapper(classBuilder, "oidType", getOidType());
        addGraphTypeWrapper(classBuilder, "vdataType", getVdataType());

        String packageName = packageElement.getQualifiedName().toString();
        writeTypeSpec(packageName, classBuilder.build());

        generateGrapeWrapper();
        graphTypeWrapperGenerated = true;
    }

    private void writeTypeSpec(String packageName, TypeSpec typeSpec) {
        JavaFile javaFile = JavaFile.builder(packageName, typeSpec).build();
        try {
            Filer filter = processingEnv.getFiler();
            javaFile.writeTo(filter);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Cannot write Java file " + javaFile.typeSpec.name + ". Please clean the build first.",
                    e);
        }
    }

    private void addGraphTypeWrapper(TypeSpec.Builder classBuilder, String name, TypeMirror type) {
        MethodSpec.Builder methodBuilder =
                MethodSpec.methodBuilder(name).addModifiers(Modifier.ABSTRACT, Modifier.PUBLIC);
        methodBuilder.addAnnotation(FFIGetter.class);
        if (!isBoxedPrimitive(type)) {
            methodBuilder.addAnnotation(CXXReference.class);
        }
        methodBuilder.returns(TypeName.get(makeGraphWrapper(type)));
        classBuilder.addMethod(methodBuilder.build());
    }

    private TypeMirror makeGraphWrapper(TypeMirror type) {
        TypeElement vectorElement =
                processingEnv.getElementUtils().getTypeElement(FFIVector.class.getName());
        TypeMirror typeVector = processingEnv.getTypeUtils().getDeclaredType(vectorElement, type);
        TypeMirror typeVectorVector =
                processingEnv.getTypeUtils().getDeclaredType(vectorElement, typeVector);
        return typeVectorVector;
    }

    private boolean isBoxedPrimitive(TypeMirror typeMirror) {
        if (isSameType(typeMirror, Byte.class)) {
            return true;
        }
        if (isSameType(typeMirror, Boolean.class)) {
            return true;
        }
        if (isSameType(typeMirror, Short.class)) {
            return true;
        }
        if (isSameType(typeMirror, Character.class)) {
            return true;
        }
        if (isSameType(typeMirror, Integer.class)) {
            return true;
        }
        if (isSameType(typeMirror, Long.class)) {
            return true;
        }
        if (isSameType(typeMirror, Float.class)) {
            return true;
        }
        if (isSameType(typeMirror, Double.class)) {
            return true;
        }
        return false;
    }

    private void output() {
        if (!checkPrecondition()) {
            return;
        }
        outputOperators();
        outputGraphType();
    }

    private boolean checkPrecondition() {
        if (graphType == null) {
            return false;
        }
        if (!checkTypes(getVidType(), getOidType(), getVdataType(), getEdataType())) {
            return false;
        }
        return true;
    }

    private boolean checkTypes(TypeMirror... typeMirrors) {
        for (TypeMirror typeMirror : typeMirrors) {
            if (!checkType(typeMirror)) {
                return false;
            }
        }
        return true;
    }

    private boolean isFFIMirror(TypeMirror typeMirror) {
        if (typeMirror instanceof DeclaredType) {
            if (!isAssignable(typeMirror, FFIType.class)) {
                return false;
            }
            DeclaredType declaredType = (DeclaredType) typeMirror;
            FFIMirror ffiMirror = declaredType.asElement().getAnnotation(FFIMirror.class);
            return ffiMirror != null;
        }
        return false;
    }

    private boolean checkType(TypeMirror typeMirror) {
        if (isFFIMirror(typeMirror)) {
            DeclaredType declaredType = (DeclaredType) typeMirror;
            String foreignType = getForeignTypeNameByFFITypeAlias(declaredType);
            FFIMirrorDefinition mirrorDefinition =
                    foreignTypeNameToFFIMirrorDefinitionMap.get(foreignType);
            if (mirrorDefinition == null) {
                // this may happen if FFIMirror is not processed by ffi-annotation-processor yet.
                return false;
            }
            return true;
        }
        return true;
    }

    private String getAnnotationMember(AnnotationMirror annotationMirror, String name) {
        Map<? extends ExecutableElement, ? extends AnnotationValue> values =
                annotationMirror.getElementValues();
        for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry :
                values.entrySet()) {
            ExecutableElement executableElement = entry.getKey();
            if (executableElement.getSimpleName().toString().equals(name)) {
                AnnotationValue value = entry.getValue();
                return value.toString();
            }
        }
        throw new IllegalStateException("Cannot find key " + name + " in " + annotationMirror);
    }

    private String getGraphTypeMember(String name) {
        String literal = getAnnotationMember(graphType, name);
        if (!literal.endsWith(".class")) {
            throw new IllegalStateException("Must be a class literal: " + literal);
        }
        return literal.substring(0, literal.length() - ".class".length());
    }

    private TypeMirror getTypeMirror(String name) {
        TypeElement typeElement = processingEnv.getElementUtils().getTypeElement(name);
        if (typeElement == null) {
            throw new IllegalStateException("Cannot get TypeElement for " + name);
        }
        return typeElement.asType();
    }

    private DeclaredType getVidType() {
        return (DeclaredType) getTypeMirror(getGraphTypeMember("vidType"));
    }

    private DeclaredType getOidType() {
        return (DeclaredType) getTypeMirror(getGraphTypeMember("oidType"));
    }

    private DeclaredType getVdataType() {
        return (DeclaredType) getTypeMirror(getGraphTypeMember("vdataType"));
    }

    private DeclaredType getEdataType() {
        return (DeclaredType) getTypeMirror(getGraphTypeMember("edataType"));
    }

    private Set<String> getGraphTypeHeaders() {
        Set<String> headers = new HashSet<>();
        Arrays.asList(getOidType(), getVidType(), getVdataType(), getEdataType()).forEach(cls -> {
            String h = getForeignTypeHeader(cls);
            if (h != null) {
                headers.add(h);
            }
        });
        return headers;
    }

    private void outputGraphType() {
        if (this.graphType == null) {
            return;
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(outputStream);
        String header = "user_type_info.h";
        String guard = AnnotationProcessorUtils.toHeaderGuard(header);
        writer.append("#ifndef ").append(guard).append("\n");
        writer.append("#define ").append(guard).append("\n");
        writer.append("#include <cstdlib>\n");
        Set<String> headers = getGraphTypeHeaders();

        headers.stream().sorted().forEach(h -> {
            writer.append("#include ").append(h).append("\n");
        });

        writer.append("\n");
        writer.append("namespace grape {\n");
        writer.format("using OID_T = %s;\n", getForeignTypeName(getOidType()));
        writer.format("using VID_T = %s;\n", getVidForeignTypeName(getVidType()));
        writer.format("using VDATA_T = %s;\n", getForeignTypeName(getVdataType()));
        writer.format("using EDATA_T = %s;\n", getForeignTypeName(getEdataType()));
        writer.append("\n\n");
        writer.format("char const* OID_T_str = \"std::vector<std::vector<%s>>\";\n",
                getForeignTypeName(getOidType()));
        writer.format("char const* VID_T_str = \"std::vector<std::vector<%s>>\";\n",
                getVidForeignTypeName(getVidType()));
        writer.format("char const* VDATA_T_str = \"std::vector<std::vector<%s>>\";\n",
                getForeignTypeName(getVdataType()));
        writer.format("char const* EDATA_T_str = \"std::vector<std::vector<%s>>\";\n",
                getForeignTypeName(getEdataType()));
        writer.append("} // end of namespace grape\n");
        writer.append("\n\n");
        writer.append("#endif // ").append(guard).append("\n");
        writer.flush();
        writeFile(header, outputStream.toString());
    }

    private String getVidForeignTypeName(TypeMirror vidType) {
        if (isSameType(vidType, Integer.class)) {
            return "uint32_t";
        }
        if (isSameType(vidType, Long.class)) {
            return "uint64_t";
        }
        throw new IllegalStateException("Unsupported vid type: " + vidType);
    }

    private void writeFile(String fileName, String fileContent) {
        try {
            FileObject fileObject =
                    processingEnv.getFiler().createResource(StandardLocation.SOURCE_OUTPUT, "", fileName);
            try (Writer headerWriter = fileObject.openWriter()) {
                headerWriter.write(fileContent);
                headerWriter.flush();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot write " + fileName + " due to " + e.getMessage(), e);
        }
    }

    private boolean isSameType(TypeMirror typeMirror, Class<?> clazz) {
        TypeMirror typeMirror2 = getTypeMirror(clazz.getName());
        return processingEnv.getTypeUtils().isSameType(typeMirror, typeMirror2);
    }

    private boolean isAssignable(TypeMirror typeMirror, Class<?> clazz) {
        TypeMirror typeMirror2 = getTypeMirror(clazz.getName());
        return processingEnv.getTypeUtils().isAssignable(typeMirror, typeMirror2);
    }

    private String getTypeName(DeclaredType declaredType) {
        return ((TypeElement) declaredType.asElement()).getQualifiedName().toString();
    }

    private String getFFIMirrorForeignTypeName(FFIMirrorDefinition mirrorDefinition) {
        String namespace = mirrorDefinition.namespace();
        String name = mirrorDefinition.name();
        if (namespace.isEmpty()) {
            return name;
        }
        return namespace + "::" + name;
    }

    private String getForeignTypeName(TypeMirror typeMirror) {
        return getForeignTypeNameOrHeader(typeMirror, true);
    }

    private String getForeignTypeHeader(TypeMirror typeMirror) {
        return getForeignTypeNameOrHeader(typeMirror, false);
    }

    private String getForeignTypeNameOrHeader(TypeMirror typeMirror, boolean name) {
        if (typeMirror instanceof PrimitiveType) {
            throw new IllegalStateException("Please use boxed type instead.");
        }
        if (typeMirror instanceof ArrayType) {
            throw new IllegalStateException("No array is supported: " + typeMirror);
        }

        if (isSameType(typeMirror, Byte.class)) {
            return name ? "jbyte" : JNI_HEADER;
        }
        if (isSameType(typeMirror, Boolean.class)) {
            return name ? "jboolean" : JNI_HEADER;
        }
        if (isSameType(typeMirror, Short.class)) {
            return name ? "jshort" : JNI_HEADER;
        }
        if (isSameType(typeMirror, Character.class)) {
            return name ? "jchar" : JNI_HEADER;
        }
        if (isSameType(typeMirror, Integer.class)) {
            return name ? "jint" : JNI_HEADER;
        }
        if (isSameType(typeMirror, Long.class)) {
            return name ? "jlong" : JNI_HEADER;
        }
        if (isSameType(typeMirror, Float.class)) {
            return name ? "jfloat" : JNI_HEADER;
        }
        if (isSameType(typeMirror, Double.class)) {
            return name ? "jdouble" : JNI_HEADER;
        }
        if (isSameType(typeMirror, FFIByteString.class)) {
            return name ? "std::string" : "<string>";
        }
        if (typeMirror instanceof DeclaredType) {
            DeclaredType declaredType = (DeclaredType) typeMirror;
            if (!declaredType.getTypeArguments().isEmpty()) {
                throw new IllegalStateException("No parameterized type is supported: " + typeMirror);
            }
            if (!isAssignable(typeMirror, FFIType.class)) {
                throw new IllegalStateException("Must be an FFIType: " + typeMirror);
            }
            FFIMirror ffiMirror = declaredType.asElement().getAnnotation(FFIMirror.class);
            if (ffiMirror == null) {
                throw new IllegalStateException("Must be an FFIMirror: " + typeMirror);
            }
            String foreignTypeName = getForeignTypeNameByFFITypeAlias(declaredType);

            FFIMirrorDefinition mirrorDefinition =
                    foreignTypeNameToFFIMirrorDefinitionMap.get(foreignTypeName);
            if (mirrorDefinition == null) {
                throw new IllegalStateException("Cannot find FFIMirror implementation for " + typeMirror
                        + "/" + declaredType.asElement() + " via foreign type "
                        + foreignTypeName);
            }
            if (!getFFIMirrorForeignTypeName(mirrorDefinition).equals(foreignTypeName)) {
                throw new IllegalStateException("Oops, expect " + foreignTypeName + ", got "
                        + mirrorDefinition.name());
            }
            return name ? getFFIMirrorForeignTypeName(mirrorDefinition)
                    : "\"" + mirrorDefinition.header() + "\"";
        }
        throw new IllegalStateException("Unsupported type: " + typeMirror);
    }

    private String getForeignTypeNameByFFITypeAlias(DeclaredType declaredType) {
        TypeElement typeElement = (TypeElement) declaredType.asElement();
        return getForeignTypeNameByFFITypeAlias(typeElement);
    }

    private String getForeignTypeNameByFFITypeAlias(TypeElement typeElement) {
        FFITypeAlias typeAlias = typeElement.getAnnotation(FFITypeAlias.class);
        FFINameSpace nameSpace = typeElement.getAnnotation(FFINameSpace.class);
        if (typeAlias == null || typeAlias.value().isEmpty()) {
            throw new IllegalStateException("No valid FFITypeAlias in " + typeElement);
        }
        if (nameSpace != null && !nameSpace.value().isEmpty()) {
            return nameSpace.value() + "::" + typeAlias.value();
        }
        return typeAlias.value();
    }

    private List<FFIMirrorFieldDefinition> sortedFields(FFIMirrorDefinition m) {
        List<FFIMirrorFieldDefinition> fields = new ArrayList<>(Arrays.asList(m.fields()));
        Collections.sort(fields, Comparator.comparing(FFIMirrorFieldDefinition::name));
        return fields;
    }

    private void outputOperators() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(outputStream);
        String grapeHeader = "grape-gen.h";
        String guard = AnnotationProcessorUtils.toHeaderGuard(grapeHeader);
        writer.append("#ifndef ").append(guard).append("\n");
        writer.append("#define ").append(guard).append("\n");
        writer.append("#include \"grape/serialization/in_archive.h\"\n");
        writer.append("#include \"grape/serialization/out_archive.h\"\n");

        List<FFIMirrorDefinition> mirrorDefinitionList =
                new ArrayList<>(foreignTypeNameToFFIMirrorDefinitionMap.values());
        Collections.sort(mirrorDefinitionList, Comparator.comparing(FFIMirrorDefinition::name));
        mirrorDefinitionList.forEach(m -> writer.format("#include \"%s\"\n", m.header()));
        mirrorDefinitionList.forEach(m -> {
            String fullName = getFFIMirrorForeignTypeName(m);
            String javaType = foreignTypeNameToJavaTypeName.get(fullName);
            if (javaType == null) {
                throw new IllegalStateException("Cannot get Java type for " + fullName);
            }
            if (javaType.endsWith(GraphTypeWrapperSuffix)) {
                return;
            }
            String namespace = m.namespace();
            if (!namespace.isEmpty()) {
                writer.format("namespace %s {\n", m.namespace());
            }
            // outarchive
            writer.format(
                    "inline grape::OutArchive& operator>>(grape::OutArchive& out_archive, %s& data) {\n",
                    m.name());
            List<FFIMirrorFieldDefinition> fields = sortedFields(m);
            fields.forEach(f -> {
                writer.format("\tout_archive >> data.%s;\n", f.name());
            });
            writer.format("\treturn out_archive;\n}\n");
            // inarchive
            writer.format(
                    "inline grape::InArchive& operator<<(grape::InArchive& in_archive, const %s& data) {\n",
                    m.name());
            fields.forEach(f -> {
                writer.format("\tin_archive << data.%s;\n", f.name());
            });
            writer.format("\treturn in_archive;\n}\n");
            // equal operator
            writer.format("inline bool operator==(const %s& a, const %s& b) {\n", m.name(), m.name());
            fields.forEach(
                    f -> {
                        writer.format("\tif (a.%s != b.%s) return false;\n", f.name(), f.name());
                    });
            writer.format("\treturn true;\n}\n");

            if (!namespace.isEmpty()) {
                writer.format("} // end namespace %s\n", m.namespace());
            }
        });

        {
            Map<String, String> all = collectFFIMirrorTypeMapping(processingEnv);
            all.forEach((foreignType, javaTypeName) -> {
                if (javaTypeName.endsWith(GraphTypeWrapperSuffix)) {
                    return;
                }
                TypeMirror javaType = AnnotationProcessorUtils.typeNameToDeclaredType(
                        processingEnv, javaTypeName, (TypeElement) null);
                if (javaType instanceof DeclaredType) {
                    DeclaredType declaredType = (DeclaredType) javaType;
                    if (!declaredType.getTypeArguments().isEmpty()) {
                        throw new IllegalStateException("No parameterized type is allowed, got " + javaType);
                    }
                    writer.append("namespace std {\n");
                    writer.format("template<> struct hash<%s> {\n", foreignType);
                    writer.format("\tstd::size_t operator()(%s const& s) const noexcept {\n", foreignType);
                    writer.append("\t\tstd::size_t h = 17;\n");
                    List<FFIMirrorFieldDefinition> fields =
                            sortedFields(foreignTypeNameToFFIMirrorDefinitionMap.get(foreignType));
                    for (FFIMirrorFieldDefinition field : fields) {
                        //llvm has no default std::hash impl for std::vector.
                        if (field.foreignType().indexOf("std::vector") == -1) {
                            writer.format("\t\th = h * 31 + std::hash<%s>()(s.%s);\n", field.foreignType(),
                                    field.name());
                        }
                    }
                    writer.append("\t\treturn h;\n");
                    writer.append("\t}\n");
                    writer.format("}; // end of std::hash<%s>\n", foreignType);
                    writer.append("} // end namespace std\n");
                }
            });
        }

        writer.append("#endif // ").append(guard).append("\n");
        writer.flush();
        writeFile(grapeHeader, outputStream.toString());
    }

    private void checkedRegisterType(ProcessingEnvironment processingEnv, String foreignType,
                                     String javaType, Map<String, String> mapping) {
        String check = mapping.put(foreignType, javaType);
        if (check != null && !check.equals(javaType)) {
            throw new IllegalStateException("Inconsistent type registered on " + foreignType
                    + ", expected " + check + ", got " + javaType);
        }
    }

    private Map<String, String> collectFFIMirrorTypeMapping(ProcessingEnvironment processingEnv) {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, FFIMirrorDefinition> entry :
                javaTypeNameToFFIMirrorDefinitionMap.entrySet()) {
            {
                String javaTypeName = entry.getKey();
                String foreignType = getFFIMirrorForeignTypeName(entry.getValue());
                checkedRegisterType(processingEnv, foreignType, javaTypeName, map);
            }
            //            for (FFIMirrorFieldDefinition mirrorFieldDefinition : entry.getValue().fields())
            //            {
            //                String javaTypeName = mirrorFieldDefinition.javaType();
            //                String foreignType = mirrorFieldDefinition.foreignType();
            //                TypeMirror javaType =
            //                AnnotationProcessorUtils.typeNameToDeclaredType(processingEnv, javaTypeName,
            //                (TypeElement) null); checkedRegisterType(processingEnv, foreignType,
            //                javaType, map);
            //            }
        }
        return map;
    }
}
