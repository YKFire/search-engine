package com.searchengine.utils;

import com.sun.xml.internal.bind.v2.TODO;

import java.util.*;


/**
 * @author: YKFire
 * @description: 实现Trie树用于，在搜索框输入文字时，提供提示内容
 * @date: 2022-06-03 19:34
 */
public class Trie {

    final int COUNT = 10;  // 提示内容的最大数目
    int cnt = 0;

    class Node {
        Map<Character, Node> child;  // 结点的所有孩子
        boolean isEnd;  // 用于判断该结点是否为结束
        Node() {
            this.child = new HashMap<>(1000);
            isEnd = false;
        }
        void addChild(char c) {
            this.child.put(c, new Node());
        }
    }

    Node root = new Node();  // 初始化一个根结点

    /**
     * @author: YKFire
     * @description: 向Trie树中添加文字
     * @date: 2022-06-03 19:38
     */
    public void add(String word) {
        Node node = root;
        int len = word.length();
        for (int i = 0; i < len; i++) {
            char c = word.charAt(i);
            if (!node.child.containsKey(c)) {
                node.addChild(c);
                if (i == len - 1) {
                    node.child.get(c).isEnd = true;
                }
            }
            node = node.child.get(c);
        }
    }

    public boolean search(String word) {
        Node node = root;
        int len = word.length();
        for (int i = 0; i < len; i++) {
            char c = word.charAt(i);
            if (!node.child.containsKey(c)) return false;
            node = node.child.get(c);
        }
        return node.isEnd;
    }

    /**
     * @author: YKFire
     * @description: 找到10个相关的词
     * @date: 2022-06-03 19:39
     */
    public List<String> getRelatedWords(String word) {
        cnt = 0;
        List<String> res = new ArrayList<>();
        Node node = root;
        int len = word.length();
        for (int i = 0; i < len; i++) {
            char c = word.charAt(i);
            if (!node.child.containsKey(c)) return null;
            node = node.child.get(c);
        }
        dfs(word, node, res, "");
        return res;
    }

    //本质上为一个深度优先搜索的方法
    // 函数的参数包括当前的单词 word，当前节点 node，结果列表 res，以及当前路径 path
    public void dfs(String word, Node node, List<String> res, String path) {  // 没必要使用StringBuilder了
        //首先判断是否已经找到了足够数量的单词，如果是则直接返回
        if (cnt >= COUNT) return;
        //然后判断当前节点是否是一个单词的结尾，如果是且这个单词不是初始单词本身，则将它加入到结果列表中，并将计数器 cnt 加 1
        if (node.isEnd && !word.equals(word + path)) {
            res.add(word + path);
            cnt++;
        }
        for (Map.Entry<Character, Node> entry : node.child.entrySet()) {
            //更新节点与路径 进行深度优先搜索
            node = entry.getValue();
            path = path + entry.getKey();
            dfs(word, node, res, path);
            //将路径 path 恢复到遍历当前节点之前的状态，以便遍历下一个子节点时继续使用
            path = path.substring(0, path.length() - 1);
        }
    }
}