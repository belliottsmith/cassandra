package org.apache.cassandra.hadoop;

import com.google.common.base.Preconditions;

import java.net.InetAddress;
import java.util.Map;

public interface IConsistencyLevel {
    boolean validateErrorMap(final Map<String, Map<InetAddress, Boolean>> errorMapByDC, final String localDC);

    boolean localCL();

    enum BulkCL implements IConsistencyLevel
    {
        ALL
        {
            @Override
            public boolean validateErrorMap(Map<String, Map<InetAddress, Boolean>> errorMapByDC, final String localDC)
            {
                for (final Map<InetAddress, Boolean> nodeErrors : errorMapByDC.values())
                {
                    for (final Boolean error : nodeErrors.values())
                    {
                        if (error)
                        {
                            return false;
                        }
                    }
                }
                return true;
            }

            @Override
            public boolean localCL()
            {
                return false;
            }
        },
        QUORUM
        {
            @Override
            public boolean validateErrorMap(Map<String, Map<InetAddress, Boolean>> errorMapByDC, final String localDC)
            {
                int succeeded = 0;
                int total = 0;
                for (final Map<InetAddress, Boolean> nodeErrors : errorMapByDC.values())
                {
                    for (final Boolean error : nodeErrors.values())
                    {
                        if (!error)
                        {
                            succeeded++;
                        }
                        total++;
                    }
                }
                return (succeeded >= (total / 2) + 1);
            }

            @Override
            public boolean localCL()
            {
                return false;
            }
        },
        EACH_QUORUM
        {
            @Override
            public boolean validateErrorMap(Map<String, Map<InetAddress, Boolean>> errorMapByDC, final String localDC)
            {
                for (final Map<InetAddress, Boolean> nodeErrors : errorMapByDC.values())
                {
                    int succeeded = 0;
                    int total = 0;
                    for (final Boolean error : nodeErrors.values())
                    {
                        if (!error)
                        {
                            succeeded++;
                        }
                        total++;
                    }
                    if (succeeded < (total / 2) + 1)
                    {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean localCL()
            {
                return false;
            }
        },
        LOCAL_QUORUM
        {
            @Override
            public boolean validateErrorMap(Map<String, Map<InetAddress, Boolean>> errorMapByDC, final String localDC)
            {
                int succeeded = 0;
                int total = 0;
                Preconditions.checkState(errorMapByDC.get(localDC) != null, "Provided local DC is not matching with Cassandra DC names");
                for (final Boolean error : errorMapByDC.get(localDC).values())
                {
                    if (!error)
                    {
                        succeeded++;
                    }
                    total++;
                }
                return (succeeded >= (total / 2) + 1);
            }

            @Override
            public boolean localCL()
            {
                return true;
            }
        },
        ONE
        {
            @Override
            public boolean validateErrorMap(Map<String, Map<InetAddress, Boolean>> errorMapByDC, String localDC)
            {
                for (final Map<InetAddress, Boolean> nodeErrors : errorMapByDC.values())
                {
                    for (final Boolean error : nodeErrors.values())
                    {
                        if (!error)
                        {
                            return true;
                        }
                    }
                }
                return false;
            }

            @Override
            public boolean localCL()
            {
                return false;
            }
        },
        LOCAL_ONE
        {
            @Override
            public boolean validateErrorMap(Map<String, Map<InetAddress, Boolean>> errorMapByDC, String localDC)
            {
                Preconditions.checkState(errorMapByDC.get(localDC) != null, "Provided local DC is not matching with Cassandra DC names");
                for (final Boolean error : errorMapByDC.get(localDC).values())
                {
                    if (!error)
                    {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean localCL()
            {
                return true;
            }
        }
    }
}