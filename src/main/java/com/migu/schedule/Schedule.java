package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;
import com.migu.schedule.node.ServerNode;
import com.migu.schedule.node.Task;

import java.util.*;

/*
 *类名和方法不能修改
 */
public class Schedule {

    public static final int waiting = -1;

    private Map<Integer, ServerNode> nodes = new HashMap<Integer, ServerNode>();

    private List<Task> waitingTasks = new ArrayList<Task>();
    private Map<Integer, Task> allTasks = new HashMap<Integer, Task>();
    private Map<Integer, Integer> taskPos = new HashMap<Integer, Integer>();

    public int init() {
        // TODO 方法未实现 测试push
        nodes.clear();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
        if (nodeId <= 0) {
            return ReturnCodeKeys.E004;
        } else if (nodes.containsKey(nodeId)) {
            return ReturnCodeKeys.E005;
        } else {
            nodes.put(nodeId, new ServerNode(nodeId));
            return ReturnCodeKeys.E003;
        }
    }

    public int unregisterNode(int nodeId) {
        if (nodeId <= 0) {
            return ReturnCodeKeys.E004;
        } else if (!nodes.containsKey(nodeId)) {
            return ReturnCodeKeys.E007;
        } else {
            ServerNode curNode = nodes.get(nodeId);
            Collection<Task> curTasks = curNode.getTasks();
            for (Task curTask : curTasks) {
                curTask.setNodeId(waiting);
                waitingTasks.add(curTask);
                taskPos.put(curTask.getTaskId(), waiting);
            }
            return ReturnCodeKeys.E006;
        }
    }


    public int addTask(int taskId, int consumption) {
        if (taskId <= 0) {
            return ReturnCodeKeys.E009;
        } else if (allTasks.containsKey(taskId)) {
            return ReturnCodeKeys.E010;
        } else {
            Task curTask = new Task(taskId, consumption);
            waitingTasks.add(curTask);
            allTasks.put(taskId, curTask);
            taskPos.put(taskId,waiting);
            return ReturnCodeKeys.E008;
        }
    }


    public int deleteTask(int taskId) {
        if (taskId <= 0) {
            return ReturnCodeKeys.E009;
        } else if (!allTasks.containsKey(taskId)) {
            return ReturnCodeKeys.E012;
        } else {
            int curNodePos = taskPos.get(taskId);
            Task curTask = allTasks.get(taskId);
            if (curNodePos == waiting) {
                waitingTasks.remove(curTask);
            } else {
                ServerNode curNode = nodes.get(curNodePos);
                curNode.deleteTask(taskId);
            }
            allTasks.remove(taskId);
            taskPos.remove(taskId);
            return ReturnCodeKeys.E011;
        }
    }


    public int scheduleTask(int threshold) {
        int codeKey = ScheduleProcesser.schedule(allTasks,nodes.keySet(),threshold);
        return codeKey;
    }



    public int queryTaskStatus(List<TaskInfo> tasks) {
        if(tasks==null)return ReturnCodeKeys.E016;
        tasks.clear();

        List<Map.Entry<Integer, Task>> infoIds = new ArrayList<Map.Entry<Integer, Task>>(allTasks.entrySet());
        // 对HashMap中的key 进行排序
        Collections.sort(infoIds, (o1, o2) -> o1.getKey().compareTo(o2.getKey()));

        for (int i = 0; i < infoIds.size(); i++) {
            tasks.add(infoIds.get(i).getValue());
        }
        return ReturnCodeKeys.E015;
    }

}
