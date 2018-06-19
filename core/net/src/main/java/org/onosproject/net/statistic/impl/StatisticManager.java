/*
 * Copyright 2014-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onosproject.net.statistic.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.GroupId;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.Link;
import org.onosproject.net.Path;

import org.onosproject.net.flow.*;
import org.onosproject.net.flow.criteria.Criterion;
import org.onosproject.net.flow.criteria.PortCriterion;
import org.onosproject.net.flow.instructions.Instruction;
import org.onosproject.net.flow.instructions.Instructions;
import org.onosproject.net.statistic.DefaultLoad;
import org.onosproject.net.statistic.Load;
import org.onosproject.net.statistic.StatisticService;
import org.onosproject.net.statistic.StatisticStore;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.slf4j.LoggerFactory.getLogger;
import static org.onosproject.security.AppGuard.checkPermission;
import static org.onosproject.security.AppPermission.Type.*;


/**
 * Provides an implementation of the Statistic Service.
 */
@Component(immediate = true)
@Service
public class StatisticManager implements StatisticService {

    private final Logger log = getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected FlowRuleService flowRuleService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected StatisticStore statisticStore;

    private static ConcurrentHashMap<FlowId, Double> flowIdRateMap=new ConcurrentHashMap<>();

    private final InternalFlowRuleListener listener = new InternalFlowRuleListener();

    private Map<FlowEntry,Long> flows_bytes = new ConcurrentHashMap<>();
    private Map<FlowEntry,Double> flows_rate = new ConcurrentHashMap<>();

    @Activate
    public void activate() {
        flowRuleService.addListener(listener);
        log.info("Started");

    }

    @Deactivate
    public void deactivate() {
        flowRuleService.removeListener(listener);
        log.info("Stopped");
    }

    @Override
    public Load load(Link link) {
        checkPermission(STATISTIC_READ);

        return load(link.src());
    }

    @Override
    public Load load(Link link, ApplicationId appId, Optional<GroupId> groupId) {
        checkPermission(STATISTIC_READ);

        Statistics stats = getStatistics(link.src());
        if (!stats.isValid()) {
            return new DefaultLoad();
        }

        ImmutableSet<FlowEntry> current = FluentIterable.from(stats.current())
                .filter(hasApplicationId(appId))
                .filter(hasGroupId(groupId))
                .toSet();
        ImmutableSet<FlowEntry> previous = FluentIterable.from(stats.previous())
                .filter(hasApplicationId(appId))
                .filter(hasGroupId(groupId))
                .toSet();

        return new DefaultLoad(aggregate(current), aggregate(previous));
    }

    @Override
    public Load load(ConnectPoint connectPoint) {
        checkPermission(STATISTIC_READ);

        return loadInternal(connectPoint);
    }

    @Override
    public Link max(Path path) {
        checkPermission(STATISTIC_READ);

        if (path.links().isEmpty()) {
            return null;
        }
        Load maxLoad = new DefaultLoad();
        Link maxLink = null;
        for (Link link : path.links()) {
            Load load = loadInternal(link.src());
            if (load.rate() > maxLoad.rate()) {
                maxLoad = load;
                maxLink = link;
            }
        }
        return maxLink;
    }

    //重载仅仅为了修改一行代码
    @Override
    public Link max(Path path, String string) {
        checkPermission(STATISTIC_READ);

        if (path.links().isEmpty()) {
            return null;
        }
        Load maxLoad = new DefaultLoad();
        Link maxLink = path.links().get(0);  //此处修改
        for (Link link : path.links()) {
            Load load = loadInternal(link.src());
            if (load.rate() > maxLoad.rate()) {
                maxLoad = load;
                maxLink = link;
            }
        }
        return maxLink;
    }

    @Override
    public Link min(Path path) {
        checkPermission(STATISTIC_READ);

        if (path.links().isEmpty()) {
            return null;
        }
        Load minLoad = new DefaultLoad();
        Link minLink = null;
        for (Link link : path.links()) {
            Load load = loadInternal(link.src());
            if (load.rate() < minLoad.rate()) {
                minLoad = load;
                minLink = link;
            }
        }
        return minLink;
    }

    @Override
    public FlowRule highestHitter(ConnectPoint connectPoint) {
        checkPermission(STATISTIC_READ);

        Set<FlowEntry> hitters = statisticStore.getCurrentStatistic(connectPoint);
        if (hitters.isEmpty()) {
            return null;
        }

        FlowEntry max = hitters.iterator().next();
        for (FlowEntry entry : hitters) {
            if (entry.bytes() > max.bytes()) {
                max = entry;
            }
        }
        return max;
    }

    private Load loadInternal(ConnectPoint connectPoint) {
        Statistics stats = getStatistics(connectPoint);
        if (!stats.isValid()) {
            return new DefaultLoad();
        }

        return new DefaultLoad(aggregate(stats.current), aggregate(stats.previous));
    }


    //zlzl=====
    //在读写map的过程中加入读写锁
    private static ReadWriteLock rwl = new ReentrantReadWriteLock();
    public void setFlowIdRateMap(FlowId flowId, Double rate)
    {
        rwl.writeLock().lock();
        try
        {
            flowIdRateMap.put(flowId, rate);
        }catch(Exception e)
        {
            e.printStackTrace();
        }finally {
            rwl.writeLock().unlock();
        }
    }

    /**
     * Returns statistics of the specified port.
     *
     * @param connectPoint port to query
     * @return statistics
     */
    private Statistics getStatistics(ConnectPoint connectPoint) {
        Set<FlowEntry> current;
        Set<FlowEntry> previous;
        synchronized (statisticStore) {
            current = getCurrentStatistic(connectPoint);
            previous = getPreviousStatistic(connectPoint);
        }

//        log.info("======connectPoint: " + connectPoint + "======current: " + current + " ======previous" + previous);

//        if(connectPoint.toString().equals("of:0000000000100000/1") || connectPoint.toString().equals("of:0000000000100000/2"))
//        {
//            log.info("======connectPoint: " + connectPoint + "======current: " + current + " ======previous" + previous);
//        }

//        for(FlowEntry flowEntry : current){
//            if(previous.contains(flowEntry)){
//                long curBytes = flowEntry.bytes();
//                for(FlowEntry flowEntry1 : previous){
//                    if(flowEntry1.id().toString().trim().equals(flowEntry.id().toString().trim())){
//                        long preBytes = flowEntry1.bytes();
//                        //log.info("statistic ----------------------------------------");
//                        //log.info("flowId: " + flowEntry.id().toString().trim());
//                        Double rate = (curBytes - preBytes)*1.0 / 5;
//                        //log.info("flowRate: " + rate);
//                        setFlowIdRateMap(flowEntry.id(), rate);
////                        writeFile("/root/onos/statsOfLinks.txt", connectPoint.toString(), flowEntry.id().toString(), rate);
//                    }
//                }
//            }
//        }
//
//        List<String> currentFlowIds = new ArrayList<>();
//        List<String> previousFlowIds = new ArrayList<>();
//        List<String> flowEntrysFlowIds = new ArrayList<>();
//        for(FlowEntry flowEntry : current){
//            currentFlowIds.add(flowEntry.id().toString());
//        }
//        for(FlowEntry flowEntry : previous){
//            previousFlowIds.add(flowEntry.id().toString());
//        }
//
//        //Iterable<FlowEntry> activeEntries = flowRuleService.getFlowEntries(srcDeviceId);
//        for(FlowEntry flowEntry : flowRuleService.getFlowEntries(connectPoint.deviceId())){
//            for (Instruction instruction : flowEntry.treatment().immediate()) {
//                if (instruction.type() == Instruction.Type.OUTPUT ) {
//                    Instructions.OutputInstruction out = (Instructions.OutputInstruction) instruction;
//                    //如果输出端口＝故障端口
//                    if (out.port().toLong() == connectPoint.port().toLong()) {
//
//                        //这里将与故障端口有关的流表项存储，便于后续计算分流策略
//                        flowEntrysFlowIds.add(flowEntry.id().toString());
//                    }
//                }
//            }
//
//        }
//
//        log.info("======connectionPoint:"+connectPoint.toString());
//        log.info("======currentFlowIds:"+currentFlowIds.toString());
//        log.info("======previousFlowIds:"+previousFlowIds.toString());
//        log.info("======flowEntryFlowIds:"+flowEntrysFlowIds.toString());

        return new Statistics(current, previous);
    }

    public void setFlowRateKV(ConcurrentHashMap<FlowId, Double> flowRate, FlowId key, Double value){
        rwl.writeLock().lock();
        try{
            flowRate.put(key, value);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            rwl.writeLock().unlock();
        }
    }
    //zlzl======hrh
//    public Map<FlowEntry,String> getFLowRate(ConnectPoint cp){
//        ConcurrentHashMap<FlowEntry,String> flowRate = new ConcurrentHashMap<>();
//        synchronized (flows_rate){
//            //log.info("====flows_rate:"+flows_rate);
//            for(FlowEntry flowEntry:flows_rate.keySet()) {
//                if (flowEntry.deviceId().toString().equals(cp.deviceId().toString())) {
//                    Instructions.OutputInstruction out = null;
//                    PortCriterion in = null;
//                    for (Instruction instruction : flowEntry.treatment().immediate()) {
//                        if (instruction.type() == Instruction.Type.OUTPUT) {
//                            out = (Instructions.OutputInstruction) instruction;
//                        }
//                    }
//                    for (Criterion criterion : flowEntry.selector().criteria()) {
//                        if (criterion.type() == Criterion.Type.IN_PORT) {
//                            in = (PortCriterion) flowEntry.selector().getCriterion(Criterion.Type.IN_PORT);
//                        }
//                    }
//                    //log.info("++++++++deviceId:"+flowEntry.deviceId()+",out:"+out.port().toString()+",in:"+in.port().toString());
//                    if ((out.port().toLong() == cp.port().toLong()) || (in.port().toLong() == cp.port().toLong())) {
//                        String flowRateString = String.valueOf(flows_rate.get(flowEntry));
//                        flowRate.put(flowEntry, flowRateString);
//                        setFlowRateKV(flowRate, flowEntry, flowRateString);
//                    }
//                }
//            }
//        }
//
////        log.info("====connectPoint:"+cp.toString());
////        for(FlowEntry f:flowRate.keySet()){
////            log.info(""+f+" = "+flowRate.get(f));
////        }
//
//        return flowRate;
//    }

    public Map<FlowId, Double> getFLowRate(ConnectPoint cp){
        ConcurrentHashMap<FlowId, Double> flowRate = new ConcurrentHashMap<>();
        synchronized (flows_rate){
            //log.info("====flows_rate:"+flows_rate);
            for(FlowEntry flowEntry:flows_rate.keySet()) {
                if (flowEntry.deviceId().toString().equals(cp.deviceId().toString())) {
                    Instructions.OutputInstruction out = null;
                    PortCriterion in = null;
                    for (Instruction instruction : flowEntry.treatment().immediate()) {
                        if (instruction.type() == Instruction.Type.OUTPUT) {
                            out = (Instructions.OutputInstruction) instruction;
                        }
                    }
                    for (Criterion criterion : flowEntry.selector().criteria()) {
                        if (criterion.type() == Criterion.Type.IN_PORT) {
                            in = (PortCriterion) flowEntry.selector().getCriterion(Criterion.Type.IN_PORT);
                        }
                    }
                    //log.info("++++++++deviceId:"+flowEntry.deviceId()+",out:"+out.port().toString()+",in:"+in.port().toString());
                    if ((out.port().toLong() == cp.port().toLong()) || (in.port().toLong() == cp.port().toLong())) {
//                        String flowRateString = String.valueOf(flows_rate.get(flowEntry));
                        //flowRate.put(flowEntry.id(), flows_rate.get(flowEntry));
                        setFlowRateKV(flowRate, flowEntry.id(), flows_rate.get(flowEntry));
                    }
                }
            }
        }

//        log.info("====connectPoint:"+cp.toString());
//        for(FlowEntry f:flowRate.keySet()){
//            log.info(""+f+" = "+flowRate.get(f));
//        }

        return flowRate;
    }


    public synchronized void caculateFLowRate(FlowRule rule){
        if(rule instanceof FlowEntry){
            for(Criterion c: rule.selector().criteria()){
                if(c.type()==Criterion.Type.IN_PORT){
                    if(flows_bytes.containsKey(rule)){
                        long preBytes = flows_bytes.get(rule);
                        long curBytes = ((FlowEntry) rule).bytes();
                        double rate = (curBytes-preBytes)/5/1000;
                        flows_rate.put((FlowEntry)rule,rate);
                    }
                    //log.info("====before instead:"+flows_bytes);
                    flows_bytes.put((FlowEntry)rule,((FlowEntry)rule).bytes());
                    //log.info("====after instead:"+flows_bytes);
                }
            }
        }
    }


    //如果可以获得map,就可以在这里写一个getMap()，然后直接通过statisticService服务来获取流速
    public Map<FlowId, Double> getFlowIdRateMap(ConnectPoint connectPoint)
    {
//        Set<FlowEntry> current;
//        Set<FlowEntry> previous;
//        synchronized (statisticStore) {
//            current = getCurrentStatistic(connectPoint);
//            previous = getPreviousStatistic(connectPoint);
//        }
//        Map<FlowId, Double> flowIdRateMap=new HashMap<>();
//        if(connectPoint.toString().equals("of:0000000000100000/1") || connectPoint.toString().equals("of:0000000000100000/2"))
//        {
//            log.info("======connectPoint: " + connectPoint + "======current: "+current+" ======previous"+previous);
//            for(FlowEntry flowEntry : current){
//                if(previous.contains(flowEntry)){
//                    long curBytes = flowEntry.bytes();
//                    for(FlowEntry flowEntry1 : previous){   //还可以用端口来进行进一步的筛选
//                        if(flowEntry1.id().toString().trim().equals(flowEntry.id().toString().trim())){
//                            long preBytes = flowEntry1.bytes();
//                            log.info("statistic ----------------------------------------");
//                            log.info("flowId: " + flowEntry.id().toString().trim());
//                            Double rate = (curBytes - preBytes)*1.0 / 5;
//                            log.info("flowRate: " + rate);
//                            flowIdRateMap.put(flowEntry.id(), rate);
//                            writeFile("/root/onos/statsOfLinks.txt", connectPoint.toString(), flowEntry.id().toString(), rate);
//                        }
//                    }
//                }
//            }
//        }

        return flowIdRateMap;
    }

    /**
     * Returns the current statistic of the specified port.

     * @param connectPoint port to query
     * @return set of flow entries
     */
    private Set<FlowEntry> getCurrentStatistic(ConnectPoint connectPoint) {
        Set<FlowEntry> stats = statisticStore.getCurrentStatistic(connectPoint);
        if (stats == null) {
            return Collections.emptySet();
        } else {
            return stats;
        }
    }

    /**
     * Returns the previous statistic of the specified port.
     *
     * @param connectPoint port to query
     * @return set of flow entries
     */
    private Set<FlowEntry> getPreviousStatistic(ConnectPoint connectPoint) {
        Set<FlowEntry> stats = statisticStore.getPreviousStatistic(connectPoint);
        if (stats == null) {
            return Collections.emptySet();
        } else {
            return stats;
        }
    }

    // TODO: make aggregation function generic by passing a function
    // (applying Java 8 Stream API?)
    /**
     * Aggregates a set of values.
     * @param values the values to aggregate
     * @return a long value
     */
    private long aggregate(Set<FlowEntry> values) {
        long sum = 0;
        for (FlowEntry f : values) {
            sum += f.bytes();
        }
        return sum;
    }

    /**
     * Internal flow rule event listener.
     */
    private class InternalFlowRuleListener implements FlowRuleListener {

        @Override
        public void event(FlowRuleEvent event) {
            FlowRule rule = event.subject();
            switch (event.type()) {
                case RULE_ADDED:
                case RULE_UPDATED:
                    caculateFLowRate(rule);
                    statisticStore.addOrUpdateStatistic((FlowEntry) rule);
                    break;
                case RULE_ADD_REQUESTED:
                    statisticStore.prepareForStatistics(rule);
                    break;
                case RULE_REMOVE_REQUESTED:
                    statisticStore.removeFromStatistics(rule);
                    break;
                case RULE_REMOVED:
                    break;
                default:
                    log.warn("Unknown flow rule event {}", event);
            }
        }
    }

    /**
     * Internal data class holding two set of flow entries.
     */
    private static class Statistics {
        private final ImmutableSet<FlowEntry> current;
        private final ImmutableSet<FlowEntry> previous;

        public Statistics(Set<FlowEntry> current, Set<FlowEntry> previous) {
            this.current = ImmutableSet.copyOf(checkNotNull(current));
            this.previous = ImmutableSet.copyOf(checkNotNull(previous));
        }

        /**
         * Returns flow entries as the current value.
         *
         * @return flow entries as the current value
         */
        public ImmutableSet<FlowEntry> current() {
            return current;
        }

        /**
         * Returns flow entries as the previous value.
         *
         * @return flow entries as the previous value
         */
        public ImmutableSet<FlowEntry> previous() {
            return previous;
        }

        /**
         * Validates values are not empty.
         *
         * @return false if either of the sets is empty. Otherwise, true.
         */
        public boolean isValid() {
            return !(current.isEmpty() || previous.isEmpty());
        }

        @Override
        public int hashCode() {
            return Objects.hash(current, previous);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Statistics)) {
                return false;
            }
            final Statistics other = (Statistics) obj;
            return Objects.equals(this.current, other.current) && Objects.equals(this.previous, other.previous);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("current", current)
                    .add("previous", previous)
                    .toString();
        }
    }

    /**
     * Creates a predicate that checks the application ID of a flow entry is the same as
     * the specified application ID.
     *
     * @param appId application ID to be checked
     * @return predicate
     */
    private static Predicate<FlowEntry> hasApplicationId(ApplicationId appId) {
        return flowEntry -> flowEntry.appId() == appId.id();
    }

    /**
     * Create a predicate that checks the group ID of a flow entry is the same as
     * the specified group ID.
     *
     * @param groupId group ID to be checked
     * @return predicate
     */
    private static Predicate<FlowEntry> hasGroupId(Optional<GroupId> groupId) {
        return flowEntry -> {
            if (!groupId.isPresent()) {
                return false;
            }
            // FIXME: The left hand type and right hand type don't match
            // FlowEntry.groupId() still returns a short value, not int.
            return flowEntry.groupId().equals(groupId.get());
        };
    }
}
