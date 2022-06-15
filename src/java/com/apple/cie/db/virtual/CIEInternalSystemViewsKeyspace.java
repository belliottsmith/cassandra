/*
 * Copyright (c) 2018-2022 Apple, Inc. All rights reserved.
 */

package com.apple.cie.db.virtual;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualTable;


public final class CIEInternalSystemViewsKeyspace extends VirtualKeyspace
{
    public static final String NAME = "cie_internal_views";

    public static CIEInternalSystemViewsKeyspace instance = new CIEInternalSystemViewsKeyspace();

    private CIEInternalSystemViewsKeyspace()
    {
        super(NAME, new ImmutableList.Builder<VirtualTable>()
                    .add(new RepairInfoTable(NAME))
                    .build());
    }
}