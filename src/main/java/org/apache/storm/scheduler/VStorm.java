package org.apache.storm.scheduler;

import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.Utils;
import org.apache.storm.shade.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.Map.Entry;

public class VStorm implements IScheduler{
    private static final Logger LOG = LoggerFactory.getLogger(EvenScheduler.class);

    private static Set<WorkerSlot> badSlots(Map<WorkerSlot, List<ExecutorDetails>> existingSlots, int numExecutors, int numWorkers) {
        if (numWorkers != 0) {
            Map<Integer, Integer> distribution = Utils.integerDivided(numExecutors, numWorkers);
            Set<WorkerSlot> slots = new HashSet<WorkerSlot>();

            for (Entry<WorkerSlot, List<ExecutorDetails>> entry : existingSlots.entrySet()) {
                Integer executorCount = entry.getValue().size();
                Integer workerCount = distribution.get(executorCount);
                if (workerCount != null && workerCount > 0) {
                    slots.add(entry.getKey());
                    workerCount--;
                    distribution.put(executorCount, workerCount);
                }
            }

            for (WorkerSlot slot : slots) {
                existingSlots.remove(slot);
            }

            return existingSlots.keySet();
        }

        return null;
    }

    public static Set<WorkerSlot> slotsCanReassign(Cluster cluster, Set<WorkerSlot> slots) {
        Set<WorkerSlot> result = new HashSet<WorkerSlot>();
        for (WorkerSlot slot : slots) {
            if (!cluster.isBlackListed(slot.getNodeId())) {
                SupervisorDetails supervisor = cluster.getSupervisorById(slot.getNodeId());
                if (supervisor != null) {
                    Set<Integer> ports = supervisor.getAllPorts();
                    if (ports != null && ports.contains(slot.getPort())) {
                        result.add(slot);
                    }
                }
            }
        }
        return result;
    }

    public static void VStorm(Topologies topologies, Cluster cluster) {
        for (TopologyDetails topology : cluster.needsSchedulingTopologies()) {

            Set<ExecutorDetails> allExecutors = topology.getExecutors();
            Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned = getAliveAssignedWorkerSlotExecutors(cluster, topology.getId());
            Set<ExecutorDetails> aliveExecutors = new HashSet<>();
            for (List<ExecutorDetails> list : aliveAssigned.values()) {
                aliveExecutors.addAll(list);
            }
            Set<WorkerSlot> canReassignSlots = slotsCanReassign(cluster, aliveAssigned.keySet());

            int totalSlotsToUse = Math.min(topology.getNumWorkers(), canReassignSlots.size() + cluster.getAvailableSlots().size());

            Set<WorkerSlot> badSlots = null;
            if (totalSlotsToUse > aliveAssigned.size() || !allExecutors.equals(aliveExecutors)) {
                badSlots = badSlots(aliveAssigned, allExecutors.size(), totalSlotsToUse);
            }
            if (badSlots != null) {
                cluster.freeSlots(badSlots);
            }
            scheduleTopologiesEvenly(new Topologies(topology), cluster);

        }
    }
    public static List<WorkerSlot> sortSlots(Cluster cluster ) {

        double power = 0;
        List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
        if(availableSlots!=null && availableSlots.size()>0) {
            List<WorkerSlot> sortedSlots = new ArrayList<>();
            List<Double> powers = new ArrayList<>();
            for (WorkerSlot slot : availableSlots) {
                SupervisorDetails supervisor = cluster.getSupervisorById(slot.getNodeId());
                power = ((0.8 * supervisor.getTotalCpu()) + (0.2 * supervisor.getTotalMemory()));
                powers.add(power);
                sortedSlots.add(slot);
            }
            for (int i = 0; i < sortedSlots.size(); i++) {
                for (int j = 1; j < sortedSlots.size(); j++) {
                    if (powers.get(j - 1) < powers.get(j)) {
                        Collections.swap(powers, j - 1, j);
                        Collections.swap(sortedSlots, j - 1, j);
                    }
                }
            }
            System.out.println("powers of slots "+sortedSlots +" is "+powers);

            return sortedSlots;
        }
        return null;
    }
    private static Map<ExecutorDetails, WorkerSlot> scheduleTopology(TopologyDetails topology, Cluster cluster) {
        List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
        Set<ExecutorDetails> allExecutors = topology.getExecutors();

        Map<String, Component> userTopologyComponents = topology.getUserTopolgyComponents();
        ArrayList<String> namesOfEachComponent = new ArrayList<String>();
        for (Component value : userTopologyComponents.values()) {
            namesOfEachComponent.add(value.getId());
        }
        namesOfEachComponent.add("__acker");
        System.out.println("Different Components: "+namesOfEachComponent);

        Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned = getAliveAssignedWorkerSlotExecutors(cluster, topology.getId());
        int totalSlotsToUse = Math.min(topology.getNumWorkers(), availableSlots.size() + aliveAssigned.size());


        List<WorkerSlot> sortedList = sortSlots(cluster);
        if (sortedList == null) {
            LOG.error("No available slots for topology: {}", topology.getName());
            return new HashMap<>();
        }
        int toIndex = Math.min((totalSlotsToUse - aliveAssigned.size()), sortedList.size());
        List<WorkerSlot> reassignSlots = sortedList.subList(0, toIndex);

        Set<ExecutorDetails> aliveExecutors = new HashSet<>();
        for (List<ExecutorDetails> list : aliveAssigned.values()) {
            aliveExecutors.addAll(list);
        }
        Set<ExecutorDetails> reassignExecutors = Sets.difference(allExecutors, aliveExecutors);

        Map<ExecutorDetails, WorkerSlot> reassignment = new HashMap<>();
        if (reassignSlots.size() == 0) {
            return reassignment;
        }

        List<ExecutorDetails> executors = new ArrayList<>(reassignExecutors);
        executors.sort(Comparator.comparingInt(ExecutorDetails::getStartTask));
        Map<ExecutorDetails, String> executorToComponent = topology.getExecutorToComponent();


        Map<String, List<ExecutorDetails>> namesIdDict = new HashMap<>();
        for (String name : namesOfEachComponent) {
            List<ExecutorDetails> idListForName = new ArrayList<>();
            for (ExecutorDetails executor : executors) {
                if (executorToComponent.get(executor).equals(name)) {
                    idListForName.add(executor);
                }
            }
            namesIdDict.put(name, idListForName);
        }
        for (String name : namesOfEachComponent) {
            int smallDivision = (int) Math.ceil(namesIdDict.get(name).size()/(double)reassignSlots.size());
            for(int i = 0,k=0;i<namesIdDict.get(name).size();i++){
                if(i % smallDivision == 0 && i!=0) {
                    k++;
                }
                reassignment.put(namesIdDict.get(name).get(i), reassignSlots.get(k));
            }

        }
        if (reassignment.size() != 0) {
            LOG.info("Available slots: {}", availableSlots);
        }
        return reassignment;
    }
    public static Map<WorkerSlot, List<ExecutorDetails>> getAliveAssignedWorkerSlotExecutors(Cluster cluster, String topologyId) {
        SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyId);
        Map<ExecutorDetails, WorkerSlot> executorToSlot = null;
        if (existingAssignment != null) {
            executorToSlot = existingAssignment.getExecutorToSlot();
        }

        return Utils.reverseMap(executorToSlot);
    }
    public static void scheduleTopologiesEvenly(Topologies topologies, Cluster cluster) {
        for (TopologyDetails topology : cluster.needsSchedulingTopologies()) {
            String topologyId = topology.getId();
            Map<ExecutorDetails, WorkerSlot> newAssignment = scheduleTopology(topology, cluster);
            Map<WorkerSlot, List<ExecutorDetails>> nodePortToExecutors = Utils.reverseMap(newAssignment);

            for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : nodePortToExecutors.entrySet()) {
                WorkerSlot nodePort = entry.getKey();
                List<ExecutorDetails> executors = entry.getValue();
                cluster.assign(nodePort, topologyId, executors);
                System.out.println("Executor "+executors.toString()+ " assigned to slot number : " + nodePort);

            }

        }
    }



    @Override
    public void prepare(Map<String, Object> conf, StormMetricsRegistry metricsRegistry) {
        //noop
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        VStorm(topologies, cluster);
    }

    @Override
    public Map<String, Map<String, Double>> config() {
        return Collections.emptyMap();
    }

}
