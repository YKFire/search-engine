
package com.searchengine.springboot.segmentation;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;
import com.searchengine.common.SegResult;
import com.searchengine.dao.RecordDao;
import com.searchengine.dao.RecordSegDao;
import com.searchengine.dao.SegmentationDao;
import com.searchengine.dao.TDao;
import com.searchengine.entity.Record;
import com.searchengine.entity.RecordSeg;
import com.searchengine.entity.Segmentation;
import com.searchengine.entity.T;
import com.searchengine.service.RecordService;
import com.searchengine.service.SegmentationService;
import com.searchengine.service.TService;
import com.searchengine.utils.jieba.keyword.Keyword;
import com.searchengine.utils.jieba.keyword.TFIDFAnalyzer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.*;


/**
 * 扫描data表把所有内容分词并加入分词库
 */
@SpringBootTest
public class addAllSeg {

    @Autowired
    private RecordService recordService;
    @Autowired
    private SegmentationService segmentationService;
    @Autowired
    private SegmentationDao segmentationDao;
    @Autowired
    private RecordSegDao recordSegDao;
    @Autowired
    private RecordDao recordDao;
    @Autowired
    private TDao tDao;

    TFIDFAnalyzer tfidfAnalyzer = new TFIDFAnalyzer();
    JiebaSegmenter jiebaSegmenter = new JiebaSegmenter();

    static HashSet<String> stopWordsSet = new HashSet<>();


    @Test
    public void addAllSeg() {

        //-----------------初始化-------------
        List<Record> records = recordService.queryAllRecord();
        List<Segmentation> segmentations = segmentationService.queryAllSeg();

        BloomFilter<String> bf = BloomFilter.create(Funnels.stringFunnel(Charset.forName("UTF-8")), 10000000);

        if (stopWordsSet == null) {
            stopWordsSet = new HashSet<>();
            loadStopWords(stopWordsSet, this.getClass().getResourceAsStream("/jieba/stop_words.txt"));
        }

        for (Segmentation seg : segmentations) {
            bf.put(seg.getWord());
        }
        //----------------初始化结束---------------


        //----------------开始加词-----------------
        for (int loop = 0; loop < 75; loop++) {

            List<String> segs = new ArrayList<>(10000);
            List<RecordSeg> relations = new ArrayList<>(10000);

            int segMaxId = segmentationDao.getMaxId();  // 获取seg表中最大的id

            for (int i = loop * 10000; i < (loop + 1) * 10000; i++) {  // 10000 15s
                Record record = records.get(i);
                String caption = record.getCaption();
                List<SegToken> segTokens = jiebaSegmenter.process(caption, JiebaSegmenter.SegMode.INDEX);
                List<Keyword> keywords = tfidfAnalyzer.analyze(caption, 5);
                Map<String, RecordSeg> countMap = new HashMap<>();
                for (SegToken segToken : segTokens) {
                    String word = segToken.word;
                    if (stopWordsSet.contains(word)) continue;//判断是否是停用词
                    int segId = 0;
                    boolean exist = false;
                    if (!bf.mightContain(word)) {  // 不存在是一定不存在
                        bf.put(word);
                        segs.add(word);
                        segId = ++segMaxId;
                        segmentations.add(new Segmentation(segMaxId, word));
                    } else {  // 但是存在不一定是真的存在，但是这种误报的可能性很小，所以这时全部遍历的时间开销是完全可以接受的。
                        // https://www.geeksforgeeks.org/bloom-filter-in-java-with-examples/ 误报概率参考，1千万分之一
                        // 需要检查一下是不是真的存在
                        for (Segmentation seg : segmentations) {
                            if (word.equals(seg.getWord())) {
                                segId = seg.getId();
                                exist = true;
                                break;
                            }
                        }
                        if (!exist) {  // 和上面的操作相同
                            bf.put(word);
                            segs.add(word);
                            segId = ++segMaxId;
                            segmentations.add(new Segmentation(segMaxId, word));
                        }
                    }

                    int dataId = record.getId();
                    double tf = 0;
                    for (Keyword v : keywords) {
                        if (v.getName().equals(word)) {
                            tf = v.getTfidfvalue();
                            break;
                        }
                    }
                    //--------------计数--------------
                    if (!countMap.containsKey(word)) {
                        int count = 1;
                        countMap.put(word, new RecordSeg(dataId, segId, tf, count));
                    } else {
                        RecordSeg t = countMap.get(word);
                        int count = t.getCount();
                        t.setCount(++count);
                        countMap.put(word, t);
                    }
                    //--------------------------------
                }
                for (RecordSeg t : countMap.values()) {
                    relations.add(t);
                }
            }

            segmentationDao.insertBatchSeg(segs);
            recordSegDao.insertBatch(relations);
        }

    }

    @Test
    /**
     * @author: YKFire
     * @description: 先单纯的添加分词表，为关系表的建立做准备 (对原方法进行了修改)
     * @date: 2023-01-08 10:53
     */
    public void addSegs() {
        // List<Record> records = recordService.queryAllRecord();
        List<String> segs = new ArrayList<>();
        //布隆过滤器
        BloomFilter<String> bf = BloomFilter.create(Funnels.stringFunnel(Charset.forName("UTF-8")), 10000000);

        //加载过滤词
        loadStopWords(stopWordsSet, this.getClass().getResourceAsStream("/jieba/stop_words.txt"));

        //获取数据库data表中的数据(大约900条左右)
        List<Record> records = recordService.selectPartialRecords(900, 0);
        //对每一条record数据进行分词
        for (int i = 0; i < 900; i++) {
            Record record = records.get(i % 90);
            String caption = record.getCaption();
            //将获取到的caption进行分词
            List<SegToken> segTokens = jiebaSegmenter.process(caption, JiebaSegmenter.SegMode.INDEX);
            for (SegToken segToken : segTokens) {
                String word = segToken.word;
                if (stopWordsSet.contains(word)) continue; // 判断是否是停用词
                if (!bf.mightContain(word)) {
                    bf.put(word);
                    segs.add(word);
                }
            }
            tDao.insert1(segs);
        }
    }

    @Test
    /**
     * @author: YKFire
     * @description: 建立数据分词之间的关系表
     *  分表按照segId的最后两位来分，这样可以保证每个表是比较均匀的。
     * @date: 2023-01-08 11:01
     */
    public void addAllSegUseSplit() {
        //获取所有的segmentations分词
        List<Segmentation> segmentations = segmentationService.queryAllSeg();
        //将分词按照 word->id 的方式放入 map
        Map<String, Integer> wordToId = new HashMap<>(900);
        for (Segmentation seg : segmentations) {
            wordToId.put(seg.getWord(), seg.getId());
        }
        //加载分词停止文件
        loadStopWords(stopWordsSet, this.getClass().getResourceAsStream("/jieba/stop_words.txt"));

        //一个List<T>代表一张data_seg_relation表（存放数据分词之前的关系）
        //mp代表多张data_seg_relation表
        Map<Integer, List<T>> mp = new HashMap<>(1);
        int cnt = 0;

        //获取data表中的数据records
        List<Record> records = recordService.selectPartialRecords(900, 0);
        //处理每一条数据record
        for (int i = 0; i < 900; i++) {
            // 取得每一条 record 并取出其中的caption
            Record record = records.get(i % 900);
            String caption = record.getCaption();

            //进行分词
            List<SegToken> segTokens = jiebaSegmenter.process(caption, JiebaSegmenter.SegMode.INDEX);

            // 获取返回的 tfidf 值最高的5个关键词
            List<Keyword> keywords = tfidfAnalyzer.analyze(caption, 5);
            Map<String, T> countMap = new HashMap<>();

            for (SegToken segToken : segTokens) {
                String word = segToken.word;

                // 判断是否是停用词
                if (stopWordsSet.contains(word)) continue;

                // 不在 segmentation 表中的分词，去掉
                if (!wordToId.containsKey(word)) continue;

                int segId = wordToId.get(word);
                int dataId = record.getId();
                double tf = 0;

                // 如果是 tfidf 值最高的5个关键词之一，就将 tf 值保存起来
                for (Keyword v : keywords) {
                    if (v.getName().equals(word)) {
                        tf = v.getTfidfvalue();
                        break;
                    }
                }
                if (!countMap.containsKey(word)) {
                    int count = 1;
                    countMap.put(word, new T(dataId, segId, tf, count));
                } else {
                    T t = countMap.get(word);
                    int count = t.getCount();
                    t.setCount(++count);
                    countMap.put(word, t);
                }
            }
            //将每一个Segment放入data_seg_relation表，但是按照Segment的Id来区分放入哪一张data_seg_relation表, idx就是区分键
            for (T t : countMap.values()) {
                int segId = t.getSegId();
                int idx = segId % 100;
                List list = mp.getOrDefault(idx, new ArrayList<>(90));
                list.add(t);
                mp.put(idx, list);
                cnt++;
            }

        }

        //最后通过 mp 来创建所有的 data_seg_relation 表
        if (cnt > 0) {
            for (Integer idx : mp.keySet()) {
                String tableName = "data_seg_relation_" + idx;
                tDao.createNewTable(tableName);
                tDao.insert2(mp.get(idx), tableName);
            }
        }
    }

    //加载停顿词
    private void loadStopWords(Set<String> set, InputStream in) {
        BufferedReader bufr;
        try {
            bufr = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = bufr.readLine()) != null) {
                set.add(line.trim());
            }
            try {
                bufr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
