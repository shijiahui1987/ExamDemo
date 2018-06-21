package com.migu.schedule;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.node.Task;

import java.util.*;

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
                addPlan(plans, taskId, nodesId);
            }
            filtByThreshold(plans, allTasks, nodesId.size(), threshold);
            if (plans.isEmpty()) return ReturnCodeKeys.E014;

            filtByAvgTask(plans);
            filtBySameConsumptionTask(plans, allTasks);

            filtByConsumptionNode(plans, allTasks);
            filtByTaskIdSorted(plans, allTasks);

            setNodesId(plans.iterator().next(), allTasks);
            return ReturnCodeKeys.E013;
        }
    }

    private static void filtByTaskIdSorted(Set<Map<Integer, Set<Integer>>> plans, Map<Integer, Task> allTasks) {
        if (plans.size() <= 1) return;
        Set<Map<Integer, Set<Integer>>> rlt = new HashSet<Map<Integer, Set<Integer>>>();

        for (Map<Integer, Set<Integer>> plan : plans) {
            Map<Integer, Integer> node2TaskCnt = new HashMap<Integer, Integer>();
            for (Integer nodeId : plan.keySet()) {
                node2TaskCnt.put(nodeId, plan.get(nodeId).size());
            }
            List<Map.Entry<Integer, Integer>> node2TaskCntSorted = new ArrayList<Map.Entry<Integer, Integer>>(node2TaskCnt.entrySet());
            // 对HashMap中的key 进行排序
            Collections.sort(node2TaskCntSorted, (o1, o2) -> o1.getValue() - o2.getValue());
            boolean badPlan = false;
            for (int i = 0; i < node2TaskCntSorted.size() - 1; i++) {
                Map.Entry<Integer, Integer> curNode = node2TaskCntSorted.get(i);
                Map.Entry<Integer, Integer> nextNode = node2TaskCntSorted.get(i + 1);
                badPlan = curNode.getValue() > nextNode.getValue();
            }
            if (badPlan) continue;
            rlt.add(plan);
        }
        plans.clear();
        plans.addAll(rlt);
    }

    private static void filtByConsumptionNode(Set<Map<Integer, Set<Integer>>> plans, Map<Integer, Task> allTasks) {
        if (plans.size() <= 1) return;
        Set<Map<Integer, Set<Integer>>> rlt = new HashSet<Map<Integer, Set<Integer>>>();
        for (Map<Integer, Set<Integer>> plan : plans) {

            Map<Integer, Integer> node2Consumption = new HashMap<Integer, Integer>();
            for (Integer nodeId : plan.keySet()) {
                int consumption = getNodeConsumption(plan.get(nodeId), allTasks);
                node2Consumption.put(nodeId, consumption);
            }
            List<Map.Entry<Integer, Integer>> node2ConsumptionSorted = new ArrayList<Map.Entry<Integer, Integer>>(node2Consumption.entrySet());
            // 对HashMap中的key 进行排序
            Collections.sort(node2ConsumptionSorted, (o1, o2) -> o1.getValue() - o2.getValue() == 0 ? o2.getKey() - o1.getKey() : o2.getValue() - o1.getValue());
            boolean badPlan = false;
            for (int i = 0; i < node2ConsumptionSorted.size() - 1; i++) {
                Map.Entry<Integer, Integer> curNode = node2ConsumptionSorted.get(i);
                int curTaskCnt = plan.get(curNode.getKey()).size();

                Map.Entry<Integer, Integer> nextNode = node2ConsumptionSorted.get(i + 1);
                int nextTaskCnt = plan.get(nextNode.getKey()).size();
                badPlan = curNode.getValue() == nextNode.getValue() && nextTaskCnt > curTaskCnt;
                badPlan = badPlan || curNode.getValue() > nextNode.getValue() && curNode.getKey() < nextNode.getKey();
            }
            if (badPlan) continue;
            rlt.add(plan);
        }

        plans.clear();
        plans.addAll(rlt);
    }

    private static int getNodeConsumption(Set<Integer> tasks, Map<Integer, Task> allTasks) {
        int rlt = 0;
        for (Integer taskId : tasks) {
            rlt += allTasks.get(taskId).getConsumption();
        }
        return rlt;

    }

    private static void filtBySameConsumptionTask(Set<Map<Integer, Set<Integer>>> plans, Map<Integer, Task> allTasks) {
        if (plans.size() <= 1) return;
        Set<Map<Integer, Set<Integer>>> rlt = new HashSet<Map<Integer, Set<Integer>>>();

        Map<Integer, Set<Integer>> sameConsumptionTasks = new HashMap<Integer, Set<Integer>>();
        for (Integer taskId : allTasks.keySet()) {
            int curConsumption = allTasks.get(taskId).getConsumption();
            Set<Integer> curSameConsumptionTasks = sameConsumptionTasks.containsKey(curConsumption) ? sameConsumptionTasks.get(curConsumption) : new HashSet<Integer>();
            curSameConsumptionTasks.add(taskId);
            sameConsumptionTasks.put(curConsumption, curSameConsumptionTasks);
        }

        Map<Integer, Set<Integer>> sameConsumptionTasksTmp = new HashMap<Integer, Set<Integer>>();
        for (Integer consumption : sameConsumptionTasks.keySet()) {
            if (sameConsumptionTasks.get(consumption).size() >= 2) {
                sameConsumptionTasksTmp.put(consumption, sameConsumptionTasks.get(consumption));
            }
        }
        sameConsumptionTasks = sameConsumptionTasksTmp;

        if (sameConsumptionTasks.isEmpty()) return;
        Map<Integer, Set<Integer>> minPlan = null;
        int minSum = Integer.MAX_VALUE;

        List<Task> sortTasks = new ArrayList<Task>(allTasks.values());
        Collections.sort(sortTasks, (o1, o2) -> o1.getTaskId() - o2.getTaskId());

        for (int i = 0; i < sortTasks.size(); i++) {
            filtBySameConsumptionTask(plans, sameConsumptionTasks, sortTasks.get(i));
        }
    }

    private static void filtBySameConsumptionTask(Set<Map<Integer, Set<Integer>>> plans, Map<Integer, Set<Integer>> sameConsumptionTasks, Task curTask) {
        int curConsumption = curTask.getConsumption();
        if (!sameConsumptionTasks.containsKey(curConsumption)) return;
        List<Integer> sortTasks = new ArrayList<Integer>(sameConsumptionTasks.get(curConsumption));
        Collections.sort(sortTasks);
        Iterator<Map<Integer, Set<Integer>>> plansIt = plans.iterator();
        Map<Map<Integer, Set<Integer>>, Integer> plan2pos = new HashMap<Map<Integer, Set<Integer>>, Integer>();

        while (plansIt.hasNext()) {
            Map<Integer, Set<Integer>> curPlan = plansIt.next();
            int pos = getLargerPos(curPlan, sortTasks);
            plan2pos.put(curPlan, pos);
        }
        List<Map.Entry<Map<Integer, Set<Integer>>, Integer>> plansPos = new ArrayList<Map.Entry<Map<Integer, Set<Integer>>, Integer>>(plan2pos.entrySet());
        // 对HashMap中的key 进行排序
        Collections.sort(plansPos, (o1, o2) -> o2.getValue() - o1.getValue());
        plans.clear();
        int minPos = plansPos.get(0).getValue();
        for (int i = 0; i < plansPos.size(); i++) {
            if (plansPos.get(i).getValue() != minPos) break;
            plans.add(plansPos.get(i).getKey());
        }
    }

    private static int getLargerPos(Map<Integer, Set<Integer>> curPlan, List<Integer> sortTasks) {
        List<Integer> sortPos = new ArrayList<Integer>(sortTasks.size());
        for (int i = 0; i < sortTasks.size(); i++) {
            int taskId = sortTasks.get(i);
            int nodeId = getTaskNodeId(curPlan, taskId);
            sortPos.add(nodeId);
        }
        for (int i = 0; i < sortPos.size() - 1; i++) {
            int curNodeId = sortPos.get(i);
            int nextNodeId = sortPos.get(i + 1);
            if (nextNodeId < curNodeId) {
                return i;
            }
        }
        return sortPos.size();
    }

    private static int getTaskNodeId(Map<Integer, Set<Integer>> plan, Integer task) {
        for (int nodeId : plan.keySet()) {
            if (plan.get(nodeId).contains(task)) return nodeId;
        }
        return 0;
    }

    private static void filtByAvgTask(Set<Map<Integer, Set<Integer>>> plans) {
        {
            if (plans.size() <= 1) return;
            Set<Map<Integer, Set<Integer>>> rlt = new HashSet<Map<Integer, Set<Integer>>>();
            int minAvgTask = Integer.MAX_VALUE;
            for (Map<Integer, Set<Integer>> plan : plans) {
                int maxTaskCnt = getMaxTask(plan);
                if (maxTaskCnt < minAvgTask) {
                    minAvgTask = maxTaskCnt;
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
        if (plan.size() <= 1) return plan.values().iterator().next().size();
        Map<Integer, Integer> planTaskCnt = new HashMap<Integer, Integer>();
        for (Integer nodeId : plan.keySet()) {
            Set<Integer> curTasks = plan.get(nodeId);
            planTaskCnt.put(nodeId, curTasks.size());
        }
        int maxTask = Integer.MIN_VALUE;
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
        for (Integer nodeId : plans.keySet()) {
            Set<Integer> tasksId = plans.get(nodeId);
            for (Integer taskId : tasksId) {
                allTasks.get(taskId).setNodeId(nodeId);
            }
        }
    }

    private static void filtByThreshold(Set<Map<Integer, Set<Integer>>> plans, Map<Integer, Task> allTasks, int nodesSize, int threshold) {
        Set<Map<Integer, Set<Integer>>> rlt = new HashSet<Map<Integer, Set<Integer>>>();
        int minConsumption = threshold;
        for (Map<Integer, Set<Integer>> plan : plans) {
            if (plan.size() != nodesSize) continue;
            int maxConsumption = getMaxConsumption(plan, allTasks, threshold);
            if (maxConsumption < minConsumption) {
                rlt.clear();
                rlt.add(plan);
                minConsumption = maxConsumption;
            } else if (maxConsumption == minConsumption) {
                rlt.add(plan);
            } else {
                continue;
            }
        }
        plans.clear();
        plans.addAll(rlt);
    }

    private static int getMaxConsumption(Map<Integer, Set<Integer>> plan, Map<Integer, Task> allTasks,
                                         int threshold) {
        Map<Integer, Integer> planConsumption = new HashMap<Integer, Integer>();
        if (plan.size() == 1) return getConsumption(plan.values().iterator().next(), allTasks);
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
        if (plans.isEmpty()) {
            for (Integer curNode : nodesId) {
                Map<Integer, Set<Integer>> newCurPlan = new HashMap<Integer, Set<Integer>>();
                Set<Integer> newCurNodeTasks = new HashSet<Integer>();
                newCurNodeTasks.add(taskId);
                newCurPlan.put(curNode, newCurNodeTasks);
                rlt.add(newCurPlan);
            }
            plans.addAll(rlt);
            return;
        }

        for (Map<Integer, Set<Integer>> curPlan : plans) {
            for (Integer curNode : nodesId) {
                Map<Integer, Set<Integer>> newCurPlan = new HashMap<Integer, Set<Integer>>();
                newCurPlan.putAll(clone(curPlan));
                Set<Integer> newCurNodeTasks = newCurPlan.containsKey(curNode) ? newCurPlan.get(curNode) : new HashSet<Integer>();
                newCurNodeTasks.add(taskId);
                newCurPlan.put(curNode, newCurNodeTasks);
                rlt.add(newCurPlan);
            }
        }
        plans.clear();
        plans.addAll(rlt);
    }

    private static Map<Integer, Set<Integer>> clone(Map<Integer, Set<Integer>> curPlan) {
        Map<Integer, Set<Integer>> rlt = new HashMap<Integer, Set<Integer>>();
        for (Integer key : curPlan.keySet()) {
            if (curPlan.get(key) == null) continue;
            Set<Integer> value = new HashSet<Integer>();
            value.addAll(curPlan.get(key));
            rlt.put(key, value);
        }
        return rlt;
    }
}
