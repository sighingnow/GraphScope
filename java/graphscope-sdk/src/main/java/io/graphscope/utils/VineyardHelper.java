package io.graphscope.utils;

public class VineyardHelper {
    private static native long objectID2Address(long objectID);

    private static native long fragGroupId2Address(long objectID);

    public static long getAddressFromObjectID(long objectID) {
        return objectID2Address(objectID);
    }

    public static long getFragGroupAddrFromObjectId(long objectID) {
        return fragGroupId2Address(objectID);
    }
}
