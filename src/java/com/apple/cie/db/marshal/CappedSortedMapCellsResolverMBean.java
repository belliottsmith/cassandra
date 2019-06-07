/*
 * Copyright (c) 2018-2019 Apple, Inc. All rights reserved.
 */

package com.apple.cie.db.marshal;

public interface CappedSortedMapCellsResolverMBean
{
    /* Disable the cell resolver - revert the behavior to an unbounded map */
    boolean getCellResolverEnabled();
    void setCellResolverEnabled(boolean enable);

    /* Get/Set the minimum effective capacity for CappedSortedMapType */
    int getMinEffectiveCap();
    void setMinEffectiveCap(int minCap);

    int getMaxEffectiveCap();
    void setMaxEffectiveCap(int maxCap);

    int getDefaultCap();
    void setDefaultCap(int defaultCap);
}
