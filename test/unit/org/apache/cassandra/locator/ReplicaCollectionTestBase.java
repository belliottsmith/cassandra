//package org.apache.cassandra.locator;
//
//import java.net.UnknownHostException;
//import java.util.Comparator;
//import java.util.List;
//import java.util.function.Predicate;
//import java.util.stream.Collectors;
//
//import com.google.common.collect.HashMultimap;
//import com.google.common.collect.ImmutableList;
//import com.google.common.collect.ImmutableSet;
//import com.google.common.collect.Iterables;
//import com.google.common.collect.Lists;
//import com.google.common.collect.Multimap;
//import org.apache.cassandra.dht.Murmur3Partitioner;
//import org.apache.cassandra.dht.Range;
//import org.apache.cassandra.dht.Token;
//import org.apache.cassandra.utils.FBUtilities;
//import org.junit.Assert;
//
//public abstract class ReplicaCollectionTestBase
//{
//    static final InetAddressAndPort EP1, EP2, EP3, BROADCAST_EP, NULL_EP;
//    static final Range<Token> R1, R2, R3, BROADCAST_RANGE, NULL_RANGE;
//
//    static
//    {
//        try
//        {
//            EP1 = InetAddressAndPort.getByName("127.0.0.1");
//            EP2 = InetAddressAndPort.getByName("127.0.0.2");
//            EP3 = InetAddressAndPort.getByName("127.0.0.3");
//            BROADCAST_EP = FBUtilities.getBroadcastAddressAndPort();
//            NULL_EP = InetAddressAndPort.getByName("127.255.255.255");
//            R1 = range(0, 1);
//            R2 = range(1, 2);
//            R3 = range(2, 3);
//            BROADCAST_RANGE = range(3, 4);
//            NULL_RANGE = range(10000, 10001);
//        }
//        catch (UnknownHostException e)
//        {
//            throw new RuntimeException(e);
//        }
//    }
//
//    static Token tk(long t)
//    {
//        return new Murmur3Partitioner.LongToken(t);
//    }
//
//    static Range<Token> range(long left, long right)
//    {
//        return new Range<>(tk(left), tk(right));
//    }
//
//    public static class TestCase<C extends ReplicaCollection<? extends C>>
//    {
//        public final ReplicaCollection<C> test;
//        public final List<Replica> canonicalList;
//        public final Multimap<InetAddressAndPort, Replica> canonicalByEndpoint;
//        public final Multimap<Range<Token>, Replica> canonicalByRange;
//
//        public TestCase(ReplicaCollection<C> test, List<Replica> canonicalList)
//        {
//            this.test = test;
//            this.canonicalList = canonicalList;
//            this.canonicalByEndpoint = HashMultimap.create();
//            this.canonicalByRange = HashMultimap.create();
//            for (Replica replica : canonicalList)
//                canonicalByEndpoint.put(replica.endpoint(), replica);
//            for (Replica replica : canonicalList)
//                canonicalByRange.put(replica.range(), replica);
//        }
//
//        public void testSize()
//        {
//            Assert.assertEquals(canonicalList.size(), test.size());
//        }
//
//        public void testEquals()
//        {
//            Assert.assertEquals(ReplicaList.copyOf(canonicalList), test);
//        }
//
//        public void testEndpoints()
//        {
//            Assert.assertEquals(ImmutableSet.copyOf(canonicalByEndpoint.keySet()), ImmutableSet.copyOf(test.endpoints()));
//        }
//
////        public void testRanges()
////        {
////            Assert.assertEquals(ImmutableSet.copyOf(canonicalByRange.keySet()), ImmutableSet.copyOf(test.ranges()));
////        }
////
//        public void testOrderOfIteration()
//        {
//            Assert.assertEquals(canonicalList, ImmutableList.copyOf(test));
//            Assert.assertEquals(canonicalList, test.stream().collect(Collectors.toList()));
//            Assert.assertEquals(Lists.transform(canonicalList, Replica::endpoint), ImmutableList.copyOf(test.endpoints()));
////            Assert.assertEquals(Lists.transform(canonicalList, Replica::range), ImmutableList.copyOf(test.ranges()));
//        }
//
//        public void testSubList(int depth)
//        {
//            Assert.assertSame(test, test.subList(0, test.size()));
//            if (test.isEmpty())
//                return;
//            TestCase<C> skipFront = new TestCase<>(test.subList(1, test.size()), canonicalList.subList(1, canonicalList.size()));
//            skipFront.testAll(depth - 1, 2, 2);
//            TestCase<C> skipBack = new TestCase<>(test.subList(0, test.size() - 1), canonicalList.subList(0, canonicalList.size() - 1));
//            skipBack.testAll(depth - 1, 2, 2);
//        }
//
//        public void testFilter(int depth)
//        {
//            Predicate<Replica> predicate = r -> r == canonicalList.get(canonicalList.size() / 2);
//            TestCase<C> filtered = new TestCase<>(test.filter(predicate), ImmutableList.copyOf(Iterables.filter(canonicalList, predicate::test)));
//            filtered.testAll(2, depth - 1, 2);
//        }
//
//        public void testContains()
//        {
//            for (Replica replica : canonicalList)
//                Assert.assertTrue(test.contains(replica));
//            Assert.assertFalse(test.contains(Replica.full(NULL_EP, NULL_RANGE)));
//        }
//
//        public void testGet()
//        {
//            for (int i = 0 ; i < canonicalList.size() ; ++i)
//                Assert.assertEquals(canonicalList.get(i), test.get(i));
//        }
//
//        public void testSort(int depth)
//        {
//            final Comparator<Replica> comparator = (o1, o2) ->
//            {
//                boolean f1 = o1 == canonicalList.get(0);
//                boolean f2 = o2 == canonicalList.get(0);
//                return f1 == f2 ? 0 : f1 ? 1 : -1;
//            };
//            TestCase<C> sorted = new TestCase<>(test.sorted(comparator), ImmutableList.sortedCopyOf(comparator, canonicalList));
//            sorted.testAll(2, 2, depth - 1);
//        }
//
//        private void testAll(int subListDepth, int filterDepth, int sortDepth)
//        {
//            testEndpoints();
////            testRanges();
//            testOrderOfIteration();
//            testContains();
//            testGet();
//            testEquals();
//            testSize();
//            if (subListDepth > 0)
//                testSubList(subListDepth);
//            if (filterDepth > 0)
//                testFilter(filterDepth);
//            if (sortDepth > 0)
//                testSort(sortDepth);
//        }
//
//        public void testAll()
//        {
//            testAll(2, 2, 2);
//        }
//    }
//
//}
