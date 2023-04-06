package com.searchengine.common;


import com.searchengine.dao.SegmentationDao;
import com.searchengine.entity.Segmentation;
import com.searchengine.utils.Trie;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

@Component
/**
 * @author: YKFire
 * @description: 将某些数据缓存到全局变量中
 * @date: 2022-06-03 18:24
 */
public class CodeCache {

    public static Trie trie = new Trie();

    @Autowired
    private SegmentationDao segmentationDao;

    //使用该注解来完成初始化，@PostConstruct注解的方法将会在依赖注入对象注入完成后被自动调用
    //执行顺序：构造方法 > @Autowired > @PostConstruct
    @PostConstruct
    public void init() {
        List<Segmentation> segmentations = segmentationDao.selectAllSeg();
        for (int i = 0; i < segmentations.size(); i++) {
            String word = segmentations.get(i).getWord();
            trie.add(word);
        }
    }
}
