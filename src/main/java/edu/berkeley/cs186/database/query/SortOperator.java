package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / (double)numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses) + getSource().estimateIOCost();
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Iterator<Record> records) {
        // TODO(proj3_part1): implement

        // use an external ArrayList to store and sort the runs
        List<Record> recordList = new ArrayList<>();
        while (records.hasNext()) {
            recordList.add(records.next());
        }
        recordList.sort(comparator);
        return makeRun(recordList);
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run
     * next.
     *
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * @return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        // TODO(proj3_part1): implement
        PriorityQueue<Pair<Record, Integer>> pq = new PriorityQueue<>(new RecordPairComparator());
        List<Iterator<Record>> iterators = new ArrayList<>();

        for (Run run : runs) {
            iterators.add(run.iterator());
        }
        int i = 0;
        // 获取每个排序段中的最小元素，并将其填入优先队列
        for (Iterator<Record> iterator : iterators) {
            if (iterator.hasNext()){
                pq.add(new Pair<>(iterator.next(), i));
            }
            i++;
        }

        //此处使用优先队列来提取要进入输出排序段的记录，同时在优先队列中，每条记录以<Record, Integer>的形式存储，整型数追踪该记录来源的排序段，
        // 每当一个记录被送到输出段，就从该记录来源的排序段再提取一个记录放入队列。
        List<Record> output = new ArrayList<>();
        while (!pq.isEmpty()) {
            Pair<Record, Integer> pair = pq.poll();
            Record record = pair.getFirst();
            output.add(record);
            Iterator<Record> it = iterators.get(pair.getSecond());
            if (it.hasNext()){
                pq.add(new Pair<>(it.next(), pair.getSecond()));
            }
        }
        return makeRun(output);
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * @return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(proj3_part1): implement

        // 调用上面的mergeSortedRuns归并函数，把已经排序的N个run，用大小为numBuffers - 1的缓冲区一次归并完成
        List<Run> mergedRuns = new ArrayList<>();
        int len = runs.size();
        int batch = numBuffers - 1;
        for (int i = 0; i + batch <= len; i += batch) {
            mergedRuns.add(mergeSortedRuns((runs.subList(i, Math.min(len, i + batch)))));
        }
        return mergedRuns;
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryOperator class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all of the source operator's records in
     * sorted order.
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();

        // TODO(proj3_part1): implement
        List<Run> sortedRuns = new ArrayList<>();

        // 外部排序的驱动方法，只需要先按缓冲区大小划分出排序段，然后为每个排序段排序(调用sortRun)，再不断归并，直到只剩下一个排序段。
        // 以下为flypig的实现，简洁但不易读，可以看这个易读的实现：https://faustpromaxpx.github.io/2022/07/05/rookie-db/#Task-2%EF%BC%9AGHJ


        // 把源迭代器内的东西加入并排好序
        while (sourceIterator.hasNext()) {
            sortedRuns.add(sortRun(getBlockIterator(sourceIterator, getSchema(), numBuffers)));
        }

        // 对排序结果进行归并
        while (sortedRuns.size() > 1) {
            sortedRuns = mergePass(sortedRuns);
        }

        // 返回所需结果
        return sortedRuns.get(0);
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

