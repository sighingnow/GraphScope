package com.alibaba.grape.annotation.test;

import com.alibaba.grape.annotation.GrapeAnnotationProcessor;
import com.google.testing.compile.Compilation;
import com.google.testing.compile.JavaFileObjects;
import org.junit.Assert;
import org.junit.Test;

import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import java.io.IOException;
import java.util.List;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static com.google.testing.compile.Compiler.javac;

public class GrapeAnnotationProcessorTest {
    static void assertEquals(String str1, String str2) {
        if (str1.length() != str2.length()) {
            throw new IllegalArgumentException("Expected length " + str1.length() + ", got "
                    + str2.length());
        }
        for (int i = 0; i < str1.length(); i++) {
            if (str1.charAt(i) != str2.charAt(i)) {
                throw new IllegalArgumentException(
                        "Expected char at " + i + " = " + str1.charAt(i) + ", got " + str2.charAt(i)
                                + ", compared = " + str1.substring(0, i + 1) + " vs. " + str2.substring(0, i + 1));
            }
        }
    }

    @Test
    public void testMirror() throws IOException {
        JavaFileObject file1 = JavaFileObjects.forSourceLines(
                "test.MirrorSample", "package test;", "import com.alibaba.ffi.*;",
                "@FFIGen(library = \"test\")", "@FFIMirror", "@FFITypeAlias(\"MirrorSample\")",
                "@FFINameSpace(\"test\")", "public interface MirrorSample extends FFIPointer {",
                "  @FFIGetter int intField();", "  @FFISetter void intField(int value);",
                "  @FFIGetter @CXXReference FFIVector<Integer> intVectorField();",
                "  @FFISetter void intVectorField(@CXXReference FFIVector<Integer> value);", "}");

        JavaFileObject file2 = JavaFileObjects.forSourceLines(
                "test.Graph", "package test;", "import com.alibaba.ffi.*;",
                "import com.alibaba.grape.annotation.*;", "@GraphType(", "  oidType = Integer.class,",
                "  vidType = Long.class,", "  vdataType = MirrorSample.class,",
                "  edataType = FFIByteString.class", ")", "public class Graph {", "}");

        {
            Compilation compilation =
                    javac()
                            .withProcessors(new com.alibaba.ffi.annotation.AnnotationProcessor(),
                                    new GrapeAnnotationProcessor())
                            .compile(file1, file2);
            checkCompilation(compilation);
        }
        {
            Compilation compilation =
                    javac()
                            .withProcessors(new GrapeAnnotationProcessor(),
                                    new com.alibaba.ffi.annotation.AnnotationProcessor())
                            .compile(file2, file1);
            checkCompilation(compilation);
        }
    }

    void checkCompilation(Compilation compilation) throws IOException {
        assertThat(compilation).succeeded();
        {
            String content = compilation.generatedFile(StandardLocation.SOURCE_OUTPUT, "grape-gen.h")
                    .get()
                    .getCharContent(true)
                    .toString();
            String expected = "#ifndef _GRAPE_GEN_H\n"
                    + "#define _GRAPE_GEN_H\n"
                    + "#include \"grape/serialization/in_archive.h\"\n"
                    + "#include \"grape/serialization/out_archive.h\"\n"
                    + "#include \"jni_test_Graph__GraphTypeDefinitions_cxx_0xd194aa06.h\"\n"
                    + "#include \"jni_test_MirrorSample_cxx_0x84c01abb.h\"\n"
                    + "namespace test {\n"
                    + "inline grape::OutArchive& operator>>(grape::OutArchive& out_archive, MirrorSample& data) {\n"
                    + "\tout_archive >> data.intField;\n"
                    + "\tout_archive >> data.intVectorField;\n"
                    + "\treturn out_archive;\n"
                    + "}\n"
                    + "inline grape::InArchive& operator<<(grape::InArchive& in_archive, const MirrorSample& data) {\n"
                    + "\tin_archive << data.intField;\n"
                    + "\tin_archive << data.intVectorField;\n"
                    + "\treturn in_archive;\n"
                    + "}\n"
                    + "inline bool operator==(const MirrorSample& a, const MirrorSample& b) {\n"
                    + "\tif (a.intField != b.intField) return false;\n"
                    + "\tif (a.intVectorField != b.intVectorField) return false;\n"
                    + "\treturn true;\n"
                    + "}\n"
                    + "} // end namespace test\n"
                    + "namespace std {\n"
                    + "template<> struct hash<test::MirrorSample> {\n"
                    + "\tstd::size_t operator()(test::MirrorSample const& s) const noexcept {\n"
                    + "\t\tstd::size_t h = 17;\n"
                    + "\t\th = h * 31 + std::hash<jint>()(s.intField);\n"
//          + "\t\th = h * 31 + std::hash<std::vector<jint>>()(s.intVectorField);\n"
                    + "\t\treturn h;\n"
                    + "\t}\n"
                    + "}; // end of std::hash<test::MirrorSample>\n"
                    + "} // end namespace std\n"
                    + "#endif // _GRAPE_GEN_H\n";
            assertEquals(content, expected);
        }
        {
            String content = compilation.generatedFile(StandardLocation.SOURCE_OUTPUT, "user_type_info.h")
                    .get()
                    .getCharContent(true)
                    .toString();
            String expected = "#ifndef _USER_TYPE_INFO_H\n"
                    + "#define _USER_TYPE_INFO_H\n"
                    + "#include <cstdlib>\n"
                    + "#include \"jni_test_MirrorSample_cxx_0x84c01abb.h\"\n"
                    + "#include <jni.h>\n"
                    + "#include <string>\n"
                    + "\n"
                    + "namespace grape {\n"
                    + "using OID_T = jint;\n"
                    + "using VID_T = uint64_t;\n"
                    + "using VDATA_T = test::MirrorSample;\n"
                    + "using EDATA_T = std::string;\n"
                    + "\n"
                    + "\n"
                    + "char const* OID_T_str = \"std::vector<std::vector<jint>>\";\n"
                    + "char const* VID_T_str = \"std::vector<std::vector<uint64_t>>\";\n"
                    + "char const* VDATA_T_str = \"std::vector<std::vector<test::MirrorSample>>\";\n"
                    + "char const* EDATA_T_str = \"std::vector<std::vector<std::string>>\";\n"
                    + "} // end of namespace grape\n"
                    + "\n"
                    + "\n"
                    + "#endif // _USER_TYPE_INFO_H\n";
            assertEquals(content, expected);
        }
        {
            String content =
                    compilation
                            .generatedFile(
                                    StandardLocation.SOURCE_OUTPUT,
                                    "test/Graph" + GrapeAnnotationProcessor.GraphTypeWrapperSuffix + ".java")
                            .get()
                            .getCharContent(true)
                            .toString();
            String expected = "package test;\n"
                    + "\n"
                    + "import com.alibaba.ffi.CXXHead;\n"
                    + "import com.alibaba.ffi.CXXReference;\n"
                    + "import com.alibaba.ffi.FFIByteString;\n"
                    + "import com.alibaba.ffi.FFIGen;\n"
                    + "import com.alibaba.ffi.FFIGetter;\n"
                    + "import com.alibaba.ffi.FFIMirror;\n"
                    + "import com.alibaba.ffi.FFIPointer;\n"
                    + "import com.alibaba.ffi.FFITypeAlias;\n"
                    + "import com.alibaba.ffi.FFIVector;\n"
                    + "import java.lang.Integer;\n"
                    + "import java.lang.Long;\n"
                    + "\n"
                    + "@FFIGen\n"
                    + "@FFIMirror\n"
                    + "@FFITypeAlias(\"Graph$$GraphTypeDefinitions\")\n"
                    + "@CXXHead(\n"
                    + "    system = \"string\"\n"
                    + ")\n"
                    + "@CXXHead(\n"
                    + "    system = \"jni.h\"\n"
                    + ")\n"
                    + "@CXXHead(\"jni_test_MirrorSample_cxx_0x84c01abb.h\")\n"
                    + "public interface Graph$$GraphTypeDefinitions extends FFIPointer {\n"
                    + "  @FFIGetter\n"
                    + "  FFIVector<FFIVector<Long>> vidType();\n"
                    + "\n"
                    + "  @FFIGetter\n"
                    + "  @CXXReference\n"
                    + "  FFIVector<FFIVector<FFIByteString>> edataType();\n"
                    + "\n"
                    + "  @FFIGetter\n"
                    + "  FFIVector<FFIVector<Integer>> oidType();\n"
                    + "\n"
                    + "  @FFIGetter\n"
                    + "  @CXXReference\n"
                    + "  FFIVector<FFIVector<MirrorSample>> vdataType();\n"
                    + "}\n";
            assertEquals(content, expected);
        }
    }

    @Test
    public void testMessageTypes() {
        GrapeAnnotationProcessor processor = new GrapeAnnotationProcessor();
        System.setProperty("grape.messageTypes", String.join(",", "jlong=" + Long.class.getName(), "jdouble=" + Double.class.getName()));
        List<String> res = processor.getMessageTypes();
//    Assert.assertEquals(Long.class.getName(), res.get(0));
//    Assert.assertEquals(Double.class.getName(), res.get(1));
        String[] types0 = processor.parseMessageType(res.get(0));
        String[] types1 = processor.parseMessageType(res.get(1));
        Assert.assertEquals("jlong", types0[0]);
        Assert.assertEquals("java.lang.Long", types0[1]);
        Assert.assertEquals("jdouble", types1[0]);
        Assert.assertEquals("java.lang.Double", types1[1]);
    }
}
