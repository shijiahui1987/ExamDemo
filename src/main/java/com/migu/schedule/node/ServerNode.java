package com.migu.schedule.node;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

//模拟服务器节点
public class ServerNode {
    private int nodeId;
    private int consumption;
    /**
     * taskid to task
     */
    private Map<Integer, Task> tasks = new HashMap<Integer, Task>();

    public ServerNode(int nodeId) {
        this.nodeId = nodeId;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public int getConsumption() {
        return consumption;
    }


    public Collection<Task> getTasks() {
        if (tasks == null || tasks.isEmpty()) {
            return Collections.emptyList();
        } else {
            return tasks.values();
        }
    }

    public void deleteTask(int taskId) {
        tasks.remove(taskId);
    }
}
