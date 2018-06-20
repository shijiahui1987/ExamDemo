package com.migu.schedule;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.node.Task;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ScheduleProcesser {
    public static int schedule(Map<Integer, Task> allTasks, Set<Integer> nodesId, int threshold) {
        if (threshold <= 0) {
            return ReturnCodeKeys.E002;
        } else if (nodesId.isEmpty() || allTasks.isEmpty()) {
            return ReturnCodeKeys.E013;
        } else if (nodesId.size() == 1) {
            int nodeId = nodesId.iterator().next();
            for (Task task : allTasks.values()) {
                task.setNodeId(nodeId);
            }
            return ReturnCodeKeys.E013;
        } else {
            Set<Map<Integer, Set<Integer>>> plans = new HashSet<Map<Integer, Set<Integer>>>();
            for (int taskId : allTasks.keySet()) {
                for (int nodeId : nodesId) {
                    addPlan(plans, taskId, nodesId);
                }
            }
            filtByThreshold(plans, allTasks, threshold);
            if (plans.isEmpty()) return ReturnCodeKeys.E014;

            filtByAvgTask(plans);
            filtBySameConsumption(plans,allTasks);
            setNodesId(plans.iterator().next(),allTasks);
            return ReturnCodeKeys.E013;
        }
    }

    private static void filtBySameConsumption(Set<Map<Integer, Set<Integer>>> plans, Map<Integer, Task> allTasks) {
        if(plans.size() == 1)return ;
        Set<Map<Integer, Set<Integer>>> rlt = new HashSet<Map<Integer, Set<Integer>>>();
        Map<Integer, Set<Integer>> sameConsumptionTasks = new HashMap<Integer, Set<Integer>>();
        for(Integer taskId : allTasks.keySet()){
            if(sameConsumptionTasks.containsKey(allTasks.get(taskId).getConsumption())){
                sameConsumptionTasks.get(allTasks.get(taskId).getConsumption()).add(taskId);
            }
        }
        for(Integer consumption : sameConsumptionTasks.keySet()){
            if(sameConsumptionTasks.get(consumption).size()<2){
                sameConsumptionTasks.remove(consumption);
            }
        }
        if(sameConsumptionTasks.isEmpty())return;
    }

    private static void filtByAvgTask(Set<Map<Integer, Set<Integer>>> plans) {
        {
            if(plans.size() == 1)return ;
            Set<Map<Integer, Set<Integer>>> rlt = new HashSet<Map<Integer, Set<Integer>>>();
            int minAvgTask = Integer.MAX_VALUE;
            for (Map<Integer, Set<Integer>> plan : plans) {
                int maxTaskCnt = getMaxTask(plan);
                if (maxTaskCnt < minAvgTask) {
                    rlt.clear();
                    rlt.add(plan);
                } else if (maxTaskCnt == minAvgTask) {
                    rlt.add(plan);
                } else {
                    continue;
                }
            }
            plans.clear();
            plans.addAll(rlt);
        }
    }

    private static int getMaxTask(Map<Integer, Set<Integer>> plan) {
        Map<Integer, Integer> planTaskCnt = new HashMap<Integer, Integer>();
        for (Integer nodeId : plan.keySet()) {
            Set<Integer> curTasks = plan.get(nodeId);
            planTaskCnt.put(nodeId, curTasks.size());
        }
        int maxTask= Integer.MIN_VALUE;
        for (Integer nodeId : planTaskCnt.keySet()) {
            for (Integer curNodeId : planTaskCnt.keySet()) {
                if (curNodeId == nodeId) continue;
                int curTask = Math.abs(planTaskCnt.get(curNodeId) - planTaskCnt.get(nodeId));
                maxTask = maxTask >= curTask ? maxTask : curTask;
            }
        }
        return maxTask;
    }

    private static void setNodesId(Map<Integer, Set<Integer>> plans, Map<Integer, Task> allTasks) {
        for(Integer nodeId :plans.keySet() ){
            Set<Integer> tasksId = plans.get(nodeId);
            for(Integer taskId :tasksId ){
                allTasks.get(taskId).setNodeId(nodeId);
            }
        }
    }

    private static void filtByThreshold(Set<Map<Integer, Set<Integer>>> plans, Map<Integer, Task> allTasks, int threshold) {
        Set<Map<Integer, Set<Integer>>> rlt = new HashSet<Map<Integer, Set<Integer>>>();
        int minConsumption = Integer.MAX_VALUE;
        for (Map<Integer, Set<Integer>> plan : plans) {
            int maxConsumption = getMaxConsumption(plan, allTasks, threshold);
            if (maxConsumption < minConsumption) {
                rlt.clear();
                rlt.add(plan);
            } else if (maxConsumption == minConsumption) {
                rlt.add(plan);
            } else {
                continue;
            }
        }
        plans.clear();
        plans.addAll(rlt);
    }

    private static int getMaxConsumption(Map<Integer, Set<Integer>> plan, Map<Integer, Task> allTasks, int threshold) {
        Map<Integer, Integer> planConsumption = new HashMap<Integer, Integer>();
        for (Integer nodeId : plan.keySet()) {
            Set<Integer> curTasks = plan.get(nodeId);
            int curConsumption = getConsumption(curTasks, allTasks);
            planConsumption.put(nodeId, curConsumption);
        }
        int maxConsumption = Integer.MIN_VALUE;
        for (Integer nodeId : planConsumption.keySet()) {
            for (Integer curNodeId : planConsumption.keySet()) {
                if (curNodeId == nodeId) continue;
                int curConsumption = Math.abs(planConsumption.get(curNodeId) - planConsumption.get(nodeId));
                maxConsumption = maxConsumption >= curConsumption ? maxConsumption : curConsumption;
                if (maxConsumption > threshold) return threshold + 1;
            }
        }
        return maxConsumption;
    }

    private static int getConsumption(Set<Integer> curTasks, Map<Integer, Task> allTasks) {
        int rlt = 0;
        for (int taskId : curTasks) {
            rlt += allTasks.get(taskId).getConsumption();
        }
        return rlt;
    }


    //nodeId 2 tasksId
    private static void addPlan(Set<Map<Integer, Set<Integer>>> plans, int taskId, Set<Integer> nodesId) {
        Set<Map<Integer, Set<Integer>>> rlt = new HashSet<Map<Integer, Set<Integer>>>();
        for (Map<Integer, Set<Integer>> curPlan : plans) {
            for (Integer curNode : curPlan.keySet()) {
                Map<Integer, Set<Integer>> newCurPlan = new HashMap<Integer, Set<Integer>>();
                newCurPlan.putAll(curPlan);
                newCurPlan.get(curNode).add(taskId);
                rlt.add(newCurPlan);
            }
        }

    }
}
