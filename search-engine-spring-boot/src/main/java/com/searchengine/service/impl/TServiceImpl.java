package com.searchengine.service.impl;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;
import com.searchengine.dao.SegmentationDao;
import com.searchengine.dao.TDao;
import com.searchengine.dto.Record;
import com.searchengine.entity.RecordSeg;
import com.searchengine.entity.Segmentation;
import com.searchengine.entity.T;
import com.searchengine.service.TService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Service
public class TServiceImpl implements TService {
    @Autowired
    private TDao tDao;
    @Autowired
    private SegmentationDao segmentationDao;
    @Override
    public boolean insert1(List<String> segs) {
        tDao.insert1(segs);
        return true;
    }
    @Override
    public boolean insert2(List<T> relations, String tableName) {
        tDao.insert2(relations, tableName);
        return true;
    }

    @Override
    public int getMaxId() {
        return tDao.getMaxId();
    }

    @Override
    public Map<String, Object> getRcord(String searchInfo, int pageSize, int pageNum) {
        // int offset = pageSize * (pageNum - 1);
        // StringBuilder sb = new StringBuilder();
        // JiebaSegmenter segmenter = new JiebaSegmenter();
        // List<SegToken> segTokens = segmenter.process(searchInfo, JiebaSegmenter.SegMode.SEARCH);
        // boolean first = true;
        // for (int i = 0; i < segTokens.size(); i++) {
        //     if (segmentationDao.selectOneSeg(segTokens.get(i).word) == null) continue;
        //     int segId = segmentationDao.selectOneSeg(segTokens.get(i).word).getId();
        //     if (first) { sb.append(segId); first = false; }
        //     else sb.append(',').append(segId);
        // }
        // System.out.println(sb.toString().equals(""));
        // if(sb.toString().equals("")){
        //     return null;
        // } else {
        //     List<Record> records = tDao.getRecord(sb.toString(), pageSize, offset);
        //     int recordsNum = tDao.getRecordsNum(sb.toString());
        //     Map<String, Object> mp = new HashMap<>();
        //     mp.put("recordsNum", recordsNum);
        //     mp.put("records", records);
        //     return mp;
        // }
        return null;
    }

    @Override
    public Map<String, Object> getRcordUseSplit(String searchInfo, int pageSize, int pageNum) {//参数分别为 搜索的关键词 一页的信息数 第几页
        //offset用于指定查询的起始位置
        int offset = pageSize * (pageNum - 1);
        //创建StringBuilder类型的字符串 用于构造sql语句
        StringBuilder sb = new StringBuilder();
        JiebaSegmenter segmenter = new JiebaSegmenter();


        //一、对用户输入的搜索词searchInfo进行过滤
        // -----处理过滤词-----stat

        //1.使用特定的切割符来分隔字符串成一个字符串数组
        // \\s+是一个正则表达式，表示一个或多个空白字符，包括空格、制表符、换行符等。
        String[] words = searchInfo.split("\\s+");

        //2.过滤搜索关键词中带有特殊符号“-”的单词
        //创建字符串集合 用于存放搜索关键词中被过滤掉的单词
        List<String> filterWord = new ArrayList<>();
        //标志位
        boolean find = false;
        int filterWordIndex = -1;
        for (int i = 0; i < words.length; i++) {
            String str = words[i];
            //匹配类似：连衣裙 -女童 这样的搜索（去除掉与女童相关的内容）
            //用于判断当前单词str是否以“-”符号开头，如果是则返回true，否则返回false
            if (Pattern.matches("^-.*?$", str)) {
                if (!find) {
                    //当匹配到第一个带有“-”符号的单词时，将其在搜索关键词中的索引位置filterWordIndex记录下来
                    filterWordIndex = searchInfo.indexOf(str);
                    find = true;
                }
                filterWord.add(str.substring(1));//返回的是字符串 str 从第二个字符开始（即去掉第一个字符'-'）直到末尾的子字符串。
            }

        }
        //截取原始搜索信息字符串中第一个过滤词之前的部分
        if (filterWordIndex != -1) {
            searchInfo = searchInfo.substring(0, filterWordIndex);
        }
        // -----处理过滤词-----end

        //三、对第一个过滤词的信息进行分词 获取分词后的集合
        List<SegToken> segTokens = segmenter.process(searchInfo, JiebaSegmenter.SegMode.SEARCH);
        boolean first = true;
        for (int i = 0; i < segTokens.size(); i++) {
            //此处修改了逻辑 先判空在查询数据库数据是否存在(一开始是反着来的)
            //判断当前字符是否为空
            if ("".equals(segTokens.get(i).word.trim())) continue;
            //判断当前分词是否存在数据库中
            Segmentation segmentation = segmentationDao.selectOneSeg(segTokens.get(i).word);
            if (segmentation == null) continue;
            int segId = segmentation.getId();
            int idx = segId % 100;
            if (first) {
                sb.append("select * from data_seg_relation_").append(idx).append(" where seg_id = ").append(segId).append('\n');
                first = false;
            } else {
                sb.append("union").append('\n');
                sb.append("select * from data_seg_relation_").append(idx).append(" where seg_id = ").append(segId).append('\n');
            }
        }
        //sql语句转化为String类型
        String info = sb.toString();
        //创建字符串filterInfo用于记录第二过滤词后的sql语句
        String filterInfo = "";
        if ("".equals(info)) return null; //如果第一个过滤词构造出来的sql为空 则直接返回(后面构造的sql也为空)
        //标志位
        boolean filterWordInSegmentation = false;
        //对原先过滤出的过滤词进行构造sql
        if (filterWord.size() > 0) {
            //清除原先的sql语句
            sb.delete(0, sb.length());
            boolean fi = true;
            for (int i = 0; i < filterWord.size(); i++) {
                if (segmentationDao.selectOneSeg(filterWord.get(i)) == null) continue;
                filterWordInSegmentation = true;
                int segId = segmentationDao.selectOneSeg(filterWord.get(i)).getId();
                int idx = segId % 100;
                if (fi) {
                    sb.append("select * from data_seg_relation_").append(idx).append(" where seg_id = ").append(segId).append('\n');
                    fi = false;
                } else {
                    sb.append("union").append('\n');
                    sb.append("select * from data_seg_relation_").append(idx).append(" where seg_id = ").append(segId).append('\n');
                }
            }
            //过滤词的sql转化为String类型
            filterInfo = sb.toString();
        }
            //创建list集合存在获取的数据内容
        List records = null;
        //存储数据量
        int recordsNum = 0;
        if (filterWord.size() > 0 && filterWordInSegmentation) {
            records = tDao.getRecordUseSplitFilter(info, filterInfo, pageSize, offset);
            recordsNum = tDao.getRecordsNumFilter(info, filterInfo);
        } else {
            records = tDao.getRecordUseSplit(info, pageSize, offset);
            recordsNum = tDao.getRecordsNum(info);
        }
        Map<String, Object> mp = new HashMap<>();
        mp.put("recordsNum", recordsNum);
        mp.put("records", records);
        return mp;
    }

    @Override
    public int createNewTable(String tableName) {
        tDao.createNewTable(tableName);
        return 0;
    }
}
