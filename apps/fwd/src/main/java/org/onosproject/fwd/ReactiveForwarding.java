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
package org.onosproject.fwd;

import com.google.common.collect.ImmutableSet;
//apache 的OSGi框架felix
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;

//onlab提供的网络数据包的类
import org.onlab.packet.Ethernet;
import org.onlab.packet.ICMP;
import org.onlab.packet.ICMP6;
import org.onlab.packet.IPv4;
import org.onlab.packet.IPv6;
import org.onlab.packet.Ip4Prefix;
import org.onlab.packet.Ip6Prefix;
import org.onlab.packet.MacAddress;
import org.onlab.packet.TCP;
import org.onlab.packet.TpPort;
import org.onlab.packet.UDP;
import org.onlab.packet.VlanId;

import org.onlab.util.KryoNamespace;
import org.onlab.util.Tools;
import org.onosproject.cfg.ComponentConfigService;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.core.DefaultApplicationId;
import org.onosproject.event.Event;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Host;
import org.onosproject.net.HostId;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.flow.*;
import org.onosproject.net.flow.criteria.Criterion;
import org.onosproject.net.flow.criteria.EthCriterion;
import org.onosproject.net.flow.criteria.PortCriterion;
import org.onosproject.net.flow.instructions.Instruction;
import org.onosproject.net.flow.instructions.Instructions;
import org.onosproject.net.flowobjective.DefaultForwardingObjective;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.flowobjective.ForwardingObjective;
import org.onosproject.net.host.HostService;
import org.onosproject.net.link.LinkEvent;
import org.onosproject.net.link.LinkListener;
import org.onosproject.net.link.LinkService;
import org.onosproject.net.packet.InboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketPriority;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.statistic.FlowStatisticService;
import org.onosproject.net.statistic.StatisticService;
import org.onosproject.net.topology.TopologyEvent;
import org.onosproject.net.topology.TopologyListener;
import org.onosproject.net.topology.TopologyService;

//onos存储相关的类
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.EventuallyConsistentMap;
import org.onosproject.store.service.MultiValuedTimestamp;
import org.onosproject.store.service.StorageService;
import org.onosproject.store.service.WallClockTimestamp;

//OSGi定义的组件的上下文环境
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
//导入的静态方法
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.onlab.util.Tools.groupedThreads;
import static org.onlab.util.Tools.maxPriority;
import static org.onosproject.net.topology.HopCountLinkWeigher.DEFAULT_HOP_COUNT_WEIGHER;
import static org.slf4j.LoggerFactory.getLogger;

//zlzl
import static org.onosproject.net.topology.HopCountLinkWeigher.DEFAULT_HOP_COUNT_WEIGHER;
import org.onosproject.core.DefaultApplicationId;
import org.onosproject.net.link.LinkService;
import org.onosproject.net.link.LinkListener;

import java.io.*;

/**
 * Sample reactive forwarding application.
 * 被动转发应用样例
 */
//声明此类作为一个组件来激活，并且强制立即激活
@Component(immediate = true)
@Service(value = ReactiveForwarding.class)
public class ReactiveForwarding {
    //默认超时时间
    private static final int DEFAULT_TIMEOUT = 10;
    //默认优先级
    private static final int DEFAULT_PRIORITY = 10;

    private final Logger log = getLogger(getClass());

    //zlzl=================
    //定义一个全局变量
    FlowId flowId_installRule = null;

    //将服务标记为应用程序所依赖的服务，应用程序激活前，保证必须有一个服务的实例被加载
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected TopologyService topologyService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected PacketService packetService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected HostService hostService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected FlowRuleService flowRuleService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected FlowObjectiveService flowObjectiveService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ComponentConfigService cfgService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected StorageService storageService;

    //zlzl========================================================
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected FlowStatisticService flowStatsService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected StatisticService statisticService;

    //与故障后的link down事件有关
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected LinkService linkService;
    //============================================================

    //本类的一个内部私有类
    private ReactivePacketProcessor processor = new ReactivePacketProcessor();

    private  EventuallyConsistentMap<MacAddress, ReactiveForwardMetrics> metrics;

    private ApplicationId appId;

    //property注解定义组件可以通过ComponentContext().getProperties()得到的属性
    //只转发packet-out消息，默认为假
    @Property(name = "packetOutOnly", boolValue = false,
            label = "Enable packet-out only forwarding; default is false")
    private boolean packetOutOnly = false;
    //第一个数据包使用OFPP_TABLE端口转发，而不是使用真实的端口，默认为假
    @Property(name = "packetOutOfppTable", boolValue = false,
            label = "Enable first packet forwarding using OFPP_TABLE port " +
                    "instead of PacketOut with actual port; default is false")
    private boolean packetOutOfppTable = false;

    //配置安装的流规则的Flow Timeout,默认是10s
    @Property(name = "flowTimeout", intValue = DEFAULT_TIMEOUT,
            label = "Configure Flow Timeout for installed flow rules; " +
                    "default is 10 sec")
    private int flowTimeout = DEFAULT_TIMEOUT;

    //配置安装的流规则的优先级，默认是10,最大的优先级
    @Property(name = "flowPriority", intValue = DEFAULT_PRIORITY,
            label = "Configure Flow Priority for installed flow rules; " +
                    "default is 10")
    private int flowPriority = DEFAULT_PRIORITY;

    //开启IPV6转发，默认为假
    @Property(name = "ipv6Forwarding", boolValue = false,
            label = "Enable IPv6 forwarding; default is false")
    private boolean ipv6Forwarding = false;

    //只匹配目的mac地址，默认为假
    @Property(name = "matchDstMacOnly", boolValue = false,
            label = "Enable matching Dst Mac Only; default is false")
    private boolean matchDstMacOnly = false;

    //匹配Vlan ID,默认为假
    @Property(name = "matchVlanId", boolValue = false,
            label = "Enable matching Vlan ID; default is false")
    private boolean matchVlanId = false;

    //匹配IPv4地址，默认为假
    @Property(name = "matchIpv4Address", boolValue = false,
            label = "Enable matching IPv4 Addresses; default is false")
    private boolean matchIpv4Address = false;

    //匹配ipv4的DSCP和ENC，默认为假
    @Property(name = "matchIpv4Dscp", boolValue = false,
            label = "Enable matching IPv4 DSCP and ECN; default is false")
    private boolean matchIpv4Dscp = false;
    //匹配Ipv6的地址，默认为假
    @Property(name = "matchIpv6Address", boolValue = false,
            label = "Enable matching IPv6 Addresses; default is false")
    private boolean matchIpv6Address = false;

    //匹配IPv6流标签，默认为假
    @Property(name = "matchIpv6FlowLabel", boolValue = false,
            label = "Enable matching IPv6 FlowLabel; default is false")
    private boolean matchIpv6FlowLabel = false;

    //匹配TCP，DUP端口号，默认为假
    @Property(name = "matchTcpUdpPorts", boolValue = false,
            label = "Enable matching TCP/UDP ports; default is false")
    private boolean matchTcpUdpPorts = false;

    //匹配ICMPv4和ICMPv6字段，默认为假
    @Property(name = "matchIcmpFields", boolValue = false,
            label = "Enable matching ICMPv4 and ICMPv6 fields; " +
                    "default is false")
    private boolean matchIcmpFields = false;

    //忽略（不转发）IP4多路广播，默认为假
    @Property(name = "ignoreIPv4Multicast", boolValue = false,
            label = "Ignore (do not forward) IPv4 multicast packets; default is false")
    private boolean ignoreIpv4McastPackets = false;

    //记录被动转发的度量，默认为假
    @Property(name = "recordMetrics", boolValue = false,
            label = "Enable record metrics for reactive forwarding")
    private boolean recordMetrics = false;

    //拓扑监听器
    private final TopologyListener topologyListener = new InternalTopologyListener();

    //仿写一个link监听器
    private final LinkListener linkListener = new InternalLinkListener();

    //java的线程执行接口
    private ExecutorService blackHoleExecutor;

    //OSGi的组件启动时自动调用的方法，
    @Activate
    public void activate(ComponentContext context) {
        //Kryo是一个快速序列化，反序列化的工具。
        //定义名称空间
        KryoNamespace.Builder metricSerializer = KryoNamespace.newBuilder()
                .register(KryoNamespaces.API)
                .register(ReactiveForwardMetrics.class)
                .register(MultiValuedTimestamp.class);
        //根据名称空间和时间戳生成存储服务，
        metrics =  storageService.<MacAddress, ReactiveForwardMetrics> eventuallyConsistentMapBuilder()
                .withName("metrics-fwd")
                .withSerializer(metricSerializer)
                .withTimestampProvider((key, metricsData) -> new
                        MultiValuedTimestamp<>(new WallClockTimestamp(), System.nanoTime()))
                .build();
        //java.util.concurrent，黑洞执行类
        blackHoleExecutor = newSingleThreadExecutor(groupedThreads("onos/app/fwd",
                                                                   "black-hole-fixer",
                                                                   log));

        cfgService.registerProperties(getClass());//ComponentConfigService
        appId = coreService.registerApplication("org.onosproject.fwd");

        packetService.addProcessor(processor, PacketProcessor.director(2));
        topologyService.addListener(topologyListener);

        //zlzl=========
        linkService.addListener(linkListener);

        readComponentConfiguration(context);
        requestIntercepts();//截取请求

        log.info("Started", appId.id());
    }

    @Deactivate
    public void deactivate() {
        cfgService.unregisterProperties(getClass(), false);
        withdrawIntercepts();//
        flowRuleService.removeFlowRulesById(appId);
        packetService.removeProcessor(processor);
        topologyService.removeListener(topologyListener);
        blackHoleExecutor.shutdown();
        blackHoleExecutor = null;
        processor = null;
        log.info("Stopped");
    }

    @Modified
    public void modified(ComponentContext context) {
        readComponentConfiguration(context);
        requestIntercepts();//截取请求
    }

    /**
     * Request packet in via packet service.
     * 通过数据包服务请求获得数据包，在inactivate方法中被调用
     */
    private void requestIntercepts() {
        //构建流量选择器
        TrafficSelector.Builder selector = DefaultTrafficSelector.builder();
        //选择以太网类型
        selector.matchEthType(Ethernet.TYPE_IPV4);
        packetService.requestPackets(selector.build(), PacketPriority.REACTIVE, appId);
        selector.matchEthType(Ethernet.TYPE_ARP);
        packetService.requestPackets(selector.build(), PacketPriority.REACTIVE, appId);

        selector.matchEthType(Ethernet.TYPE_IPV6);
        if (ipv6Forwarding) {
            packetService.requestPackets(selector.build(), PacketPriority.REACTIVE, appId);
        } else {
            packetService.cancelPackets(selector.build(), PacketPriority.REACTIVE, appId);
        }
    }

    /**
     * Cancel request for packet in via packet service.
     * 取消数据包服务的拦截请求，在deactivate方法中被调用
     */
    private void withdrawIntercepts() {
        TrafficSelector.Builder selector = DefaultTrafficSelector.builder();
        selector.matchEthType(Ethernet.TYPE_IPV4);
        packetService.cancelPackets(selector.build(), PacketPriority.REACTIVE, appId);
        selector.matchEthType(Ethernet.TYPE_ARP);
        packetService.cancelPackets(selector.build(), PacketPriority.REACTIVE, appId);
        selector.matchEthType(Ethernet.TYPE_IPV6);
        packetService.cancelPackets(selector.build(), PacketPriority.REACTIVE, appId);
    }

    /**
     * Extracts properties from the component configuration context.
     *从component配置上下文中提取属性,在invactivate方法中被调用
     * @param context the component context
     */
    private void readComponentConfiguration(ComponentContext context) {
        Dictionary<?, ?> properties = context.getProperties();

        Boolean packetOutOnlyEnabled =
                Tools.isPropertyEnabled(properties, "packetOutOnly");
        if (packetOutOnlyEnabled == null) {
            log.info("Packet-out is not configured, " +
                     "using current value of {}", packetOutOnly);
        } else {
            packetOutOnly = packetOutOnlyEnabled;
            log.info("Configured. Packet-out only forwarding is {}",
                    packetOutOnly ? "enabled" : "disabled");
        }

        Boolean packetOutOfppTableEnabled =
                Tools.isPropertyEnabled(properties, "packetOutOfppTable");
        if (packetOutOfppTableEnabled == null) {
            log.info("OFPP_TABLE port is not configured, " +
                     "using current value of {}", packetOutOfppTable);
        } else {
            packetOutOfppTable = packetOutOfppTableEnabled;
            log.info("Configured. Forwarding using OFPP_TABLE port is {}",
                    packetOutOfppTable ? "enabled" : "disabled");
        }

        Boolean ipv6ForwardingEnabled =
                Tools.isPropertyEnabled(properties, "ipv6Forwarding");
        if (ipv6ForwardingEnabled == null) {
            log.info("IPv6 forwarding is not configured, " +
                     "using current value of {}", ipv6Forwarding);
        } else {
            ipv6Forwarding = ipv6ForwardingEnabled;
            log.info("Configured. IPv6 forwarding is {}",
                    ipv6Forwarding ? "enabled" : "disabled");
        }

        Boolean matchDstMacOnlyEnabled =
                Tools.isPropertyEnabled(properties, "matchDstMacOnly");
        if (matchDstMacOnlyEnabled == null) {
            log.info("Match Dst MAC is not configured, " +
                     "using current value of {}", matchDstMacOnly);
        } else {
            matchDstMacOnly = matchDstMacOnlyEnabled;
            log.info("Configured. Match Dst MAC Only is {}",
                    matchDstMacOnly ? "enabled" : "disabled");
        }

        Boolean matchVlanIdEnabled =
                Tools.isPropertyEnabled(properties, "matchVlanId");
        if (matchVlanIdEnabled == null) {
            log.info("Matching Vlan ID is not configured, " +
                     "using current value of {}", matchVlanId);
        } else {
            matchVlanId = matchVlanIdEnabled;
            log.info("Configured. Matching Vlan ID is {}",
                    matchVlanId ? "enabled" : "disabled");
        }

        Boolean matchIpv4AddressEnabled =
                Tools.isPropertyEnabled(properties, "matchIpv4Address");
        if (matchIpv4AddressEnabled == null) {
            log.info("Matching IPv4 Address is not configured, " +
                     "using current value of {}", matchIpv4Address);
        } else {
            matchIpv4Address = matchIpv4AddressEnabled;
            log.info("Configured. Matching IPv4 Addresses is {}",
                    matchIpv4Address ? "enabled" : "disabled");
        }

        Boolean matchIpv4DscpEnabled =
                Tools.isPropertyEnabled(properties, "matchIpv4Dscp");
        if (matchIpv4DscpEnabled == null) {
            log.info("Matching IPv4 DSCP and ECN is not configured, " +
                     "using current value of {}", matchIpv4Dscp);
        } else {
            matchIpv4Dscp = matchIpv4DscpEnabled;
            log.info("Configured. Matching IPv4 DSCP and ECN is {}",
                    matchIpv4Dscp ? "enabled" : "disabled");
        }

        Boolean matchIpv6AddressEnabled =
                Tools.isPropertyEnabled(properties, "matchIpv6Address");
        if (matchIpv6AddressEnabled == null) {
            log.info("Matching IPv6 Address is not configured, " +
                     "using current value of {}", matchIpv6Address);
        } else {
            matchIpv6Address = matchIpv6AddressEnabled;
            log.info("Configured. Matching IPv6 Addresses is {}",
                    matchIpv6Address ? "enabled" : "disabled");
        }

        Boolean matchIpv6FlowLabelEnabled =
                Tools.isPropertyEnabled(properties, "matchIpv6FlowLabel");
        if (matchIpv6FlowLabelEnabled == null) {
            log.info("Matching IPv6 FlowLabel is not configured, " +
                     "using current value of {}", matchIpv6FlowLabel);
        } else {
            matchIpv6FlowLabel = matchIpv6FlowLabelEnabled;
            log.info("Configured. Matching IPv6 FlowLabel is {}",
                    matchIpv6FlowLabel ? "enabled" : "disabled");
        }

        Boolean matchTcpUdpPortsEnabled =
                Tools.isPropertyEnabled(properties, "matchTcpUdpPorts");
        if (matchTcpUdpPortsEnabled == null) {
            log.info("Matching TCP/UDP fields is not configured, " +
                     "using current value of {}", matchTcpUdpPorts);
        } else {
            matchTcpUdpPorts = matchTcpUdpPortsEnabled;
            log.info("Configured. Matching TCP/UDP fields is {}",
                    matchTcpUdpPorts ? "enabled" : "disabled");
        }

        Boolean matchIcmpFieldsEnabled =
                Tools.isPropertyEnabled(properties, "matchIcmpFields");
        if (matchIcmpFieldsEnabled == null) {
            log.info("Matching ICMP (v4 and v6) fields is not configured, " +
                     "using current value of {}", matchIcmpFields);
        } else {
            matchIcmpFields = matchIcmpFieldsEnabled;
            log.info("Configured. Matching ICMP (v4 and v6) fields is {}",
                    matchIcmpFields ? "enabled" : "disabled");
        }

        Boolean ignoreIpv4McastPacketsEnabled =
                Tools.isPropertyEnabled(properties, "ignoreIpv4McastPackets");
        if (ignoreIpv4McastPacketsEnabled == null) {
            log.info("Ignore IPv4 multi-cast packet is not configured, " +
                     "using current value of {}", ignoreIpv4McastPackets);
        } else {
            ignoreIpv4McastPackets = ignoreIpv4McastPacketsEnabled;
            log.info("Configured. Ignore IPv4 multicast packets is {}",
                    ignoreIpv4McastPackets ? "enabled" : "disabled");
        }
        Boolean recordMetricsEnabled =
                Tools.isPropertyEnabled(properties, "recordMetrics");
        if (recordMetricsEnabled == null) {
            log.info("IConfigured. Ignore record metrics  is {} ," +
                    "using current value of {}", recordMetrics);
        } else {
            recordMetrics = recordMetricsEnabled;
            log.info("Configured. record metrics  is {}",
                    recordMetrics ? "enabled" : "disabled");
        }

        flowTimeout = Tools.getIntegerProperty(properties, "flowTimeout", DEFAULT_TIMEOUT);
        log.info("Configured. Flow Timeout is configured to {} seconds", flowTimeout);

        flowPriority = Tools.getIntegerProperty(properties, "flowPriority", DEFAULT_PRIORITY);
        log.info("Configured. Flow Priority is configured to {}", flowPriority);
    }


    //存储flowId和path(path可以轻松拿到,flowId或需要借助payload)
    //看来变量定义在内部类外面也可以
    Map<FlowId, Path> flowIdPathMap = new HashMap<>();  //重要是要分配内存！,千万不能赋值为null，不然会报异常，因为没有提前分配内存

    /**
     * Packet processor responsible for forwarding packets along their paths.
     * 负责在他们路径上的数据包的处理
     */
    private class ReactivePacketProcessor implements PacketProcessor {

        @Override
        public void process(PacketContext context) {
            // Stop processing if the packet has been handled, since we
            // can't do any more to it.

            if (context.isHandled()) {
                return;
            }

            InboundPacket pkt = context.inPacket();
            Ethernet ethPkt = pkt.parsed();//从数据包中解析出以太网的信息

            if (ethPkt == null) {
                return;
            }

            MacAddress macAddress = ethPkt.getSourceMAC();
            ReactiveForwardMetrics macMetrics = null;
            macMetrics = createCounter(macAddress);
            inPacket(macMetrics);//增加packet_in数据包的计数

            // Bail if this is deemed to be a control packet.如果这被认为是一个控制包，释放。
            if (isControlPacket(ethPkt)) {
                droppedPacket(macMetrics);//只是增加丢弃数据包的计数
                return;
            }

            // Skip IPv6 multicast packet when IPv6 forward is disabled.
            if (!ipv6Forwarding && isIpv6Multicast(ethPkt)) {
                droppedPacket(macMetrics);
                return;
            }
            //HostId包含Mac和VlanId
            HostId id = HostId.hostId(ethPkt.getDestinationMAC());//得到目的主机的mac

            // Do not process LLDP MAC address in any way.不处理LLDP的mac地址
            if (id.mac().isLldp()) {
                droppedPacket(macMetrics);
                return;
            }

            // Do not process IPv4 multicast packets, let mfwd handle them，不处理IPv4多播数据包
            if (ignoreIpv4McastPackets && ethPkt.getEtherType() == Ethernet.TYPE_IPV4) {
                if (id.mac().isMulticast()) {
                    return;
                }
            }

            // Do we know who this is for? If not, flood and bail.如果主机服务中没有这个主机，flood然后丢弃
            Host dst = hostService.getHost(id);
            if (dst == null) {
                flood(context, macMetrics);
                return;
            }

            // Are we on an edge switch that our destination is on? If so,
            // simply forward out to the destination and bail.如果是目的主机链接的边缘交换机发过来的，简单安装流规则，然后释放。
            if (pkt.receivedFrom().deviceId().equals(dst.location().deviceId())) {
                if (!context.inPacket().receivedFrom().port().equals(dst.location().port())) {
                    installRule(context, dst.location().port(), macMetrics);
                }
                return;
            }

            // Otherwise, get a set of paths that lead from here to the
            // destination edge switch.如果不是边缘交换机，则通过拓扑服务，得到从这里到达目地边缘交换机的路径集合。
            Set<Path> paths =
                    topologyService.getPaths(topologyService.currentTopology(),
                                             pkt.receivedFrom().deviceId(),
                                             dst.location().deviceId());
            if (paths.isEmpty()) {
                // If there are no paths, flood and bail.//如果得到的路径为空，则flood然后释放
                flood(context, macMetrics);
                return;
            }

            // Otherwise, pick a path that does not lead back to where we
            // came from; if no such path, flood and bail.如果存在路径的话，从给定集合中选择一条不返回指定端口的路径。
            Path path = pickForwardPathIfPossible(paths, pkt.receivedFrom().port());
            if (path == null) {
                log.warn("Don't know where to go from here {} for {} -> {}",
                         pkt.receivedFrom(), ethPkt.getSourceMAC(), ethPkt.getDestinationMAC());
                flood(context, macMetrics);
                return;
            }

            // Otherwise forward and be done with it.最后安装流规则
            installRule(context, path.src().port(), macMetrics);


            if (flowId_installRule==null)
            {
                log.info("=========flowid为空======================================================================");
            }else {
                //log.info("=========installRule中生成的flow_id：＝＝＝＝＝:"+flowId_installRule);
                //为每条link安装备用路径
                //installBackupPathForLink(path, context, flowId_installRule);
                //log.info("=============安装备用路径后，计算出的flowIdMap: " + flowIdPathMap.toString());
//                writeObject(flowIdPathMap);
//                writeObjectToFile();    //同时测试Map<String,String>
            }

            //获取流速(仅测试用)
            //getFlowRate();

        }

    }

    //为每条link安装备用路径
    private void installBackupPathForLink(Path path, PacketContext context, FlowId flowId) {
        /**.
         *  zlzl
         * 当未知流第一次数请求控制器时，控制器为该条分配一条path,我需要为该path的每条link设置一条可替代路径(目前只能做到为每条流设置备用路径)
         * 如何拿到路径的剩余容量
         * 拿到该path之间所有的links,
         *      取某条link,取该link两端的mac地址，为该源目地址对用低优先级流表设置替代路径
         *
         * 可根据link取得两端的mac地址和port号,以及从该故障端口输出的所有流表项，使其失效
         * 可以拿到流表项的入端口
         * 拿到在其中某台交换机中的所有流表项数量
         * 拿到流表项信息，获取优先级（默认为10）
         *
         * ------------------------
         * 流表项id和流id是否一样
         *
         */

        List<Link> pathLinks = path.links();

        //需要确定flowId,可以通过各种方式(例如五元组，可以做到)
        Link curLink = pathLinks.get(0);
        //log.info("======第===" + 0 + "===条link( " + curLink.src().deviceId().toString() + "," + curLink.dst().deviceId().toString() + " )");
        DeviceId srcLinkDeviceId = curLink.src().deviceId();
        DeviceId dstLinkDeviceId = curLink.dst().deviceId();

        Set<Path> anotherPaths = topologyService.getKShortestPaths(topologyService.currentTopology(), srcLinkDeviceId, dstLinkDeviceId, DEFAULT_HOP_COUNT_WEIGHER, 2);
        log.info("=====link( " + srcLinkDeviceId + "," + dstLinkDeviceId + " )======anotherPaths.size():" + anotherPaths.size());
        //为该link再次计算一条候选路径，注意流表项的优先级
        if (anotherPaths.size() == 0) {
            log.info("=====link( " + srcLinkDeviceId + "," + dstLinkDeviceId + " )==出错了，无法找到任何一条路径====");
        } else if (anotherPaths.size() == 1) {
            log.info("=====link( " + srcLinkDeviceId + "," + dstLinkDeviceId + " )==仅有一条路径，没有备份路径====");
        } else {
            int k = 1;
            for (Path path1 : anotherPaths) {
                //找尽可能短的ｋ条link
//              log.info("====这是第"+ k +"条备份路径,有"+ path1.links().size()+"条link");
                if (path1.links().size() == 3) {
                    //log.info("\n=========link( " + srcLinkDeviceId + "," + dstLinkDeviceId + " 数目为３的备份路径为:"+path1.toString());
                    //log.info("=================准备形成flowIdPathMap，大小为：" + flowIdPathMap.size());
                    //DefaultPath dp = new DefaultPath(null,path1.links(),path1.cost());
//                    log.info("==========================dp:"+ dp.toString());

                    //放进map对
                    flowIdPathMap.put(flowId, path1);

                    //所谓的installRule()　不过是传递了三个参数：一个的数据包上下文，一个是端口号，一个是mac地址度量，应该不会影响到数据包的流转
                    //循环下发，就可以将流表项安装到备份路径上的每个交换机
                    for (int j = 0; j < path1.links().size(); j = j + 1) {
                        Link curBackupLink = path1.links().get(j);

                        //分开考虑是考虑到流表项的进出端口的配置问题：除第一条流表项之外，后续流表项的进端口前一条link的源端口，后续流表项的出端口是当前link的目地端口
                        if(j==0)
                        {
                            PortNumber In_port = context.inPacket().receivedFrom().port();
                            installBackupRule(context, curBackupLink.src().port(), In_port, curBackupLink.src().deviceId(), 9);
                        }
                        else {
                            Link preBackupLink = path1.links().get(j-1);
                            //安装备份路径
                            installBackupRule(context, curBackupLink.src().port(), preBackupLink.dst().port(), curBackupLink.src().deviceId(), 9);
                        }
                    }

                    break;
                }
                k = k + 1;
            }
        }
    }


    //主动安装流表项(也并不是在这个文件中主动安装流表项)　　ok
    private void installRuleInActive(FlowEntry flowentry, Path path)
    {
        //安装备份路径(采取主动安装流表项的方式)
        //TrafficSelector ts = flowentry.selector();      //创建一个selector，需要修改输入端口,即IN_PORT
        //TrafficTreatment tt = flowentry.treatment();    //创建一个treatment，需要修改输出端口,即OUTPUT

        for(int j = 0; j < path.links().size(); j = j + 1)
        {
            if(j==0)
            {
                //第一条流表项，单独考虑(因为进端口,第一条流表项的源端是获取到流表项的源端)
                PortCriterion inPortCriterion = (PortCriterion)flowentry.selector().getCriterion(Criterion.Type.IN_PORT);
                PortNumber In_port = inPortCriterion.port();
                PortNumber Out_port = path.links().get(0).src().port();
                DeviceId curDevice = path.links().get(0).src().deviceId();  //每一条link的源端交换机

                TrafficSelector ts = DefaultTrafficSelector.builder(flowentry.selector()).matchInPort(In_port).build();    //创建一个selector
                TrafficTreatment tt = DefaultTrafficTreatment.builder().add(Instructions.createOutput(Out_port)).build();
                FlowRule newFLowRule = new DefaultFlowRule(curDevice, ts, tt, 10, new DefaultApplicationId(flowentry.appId(), "zlly"), 50000, false, flowentry.payLoad());
                flowRuleService.applyFlowRules(newFLowRule);

            }else {
                //进端口
                PortNumber In_port = path.links().get(j-1).dst().port();
                PortNumber Out_port = path.links().get(j).src().port();
                DeviceId curDevice = path.links().get(j).src().deviceId();

                TrafficSelector ts = DefaultTrafficSelector.builder(flowentry.selector()).matchInPort(In_port).build();    //创建一个selector
                TrafficTreatment tt = DefaultTrafficTreatment.builder().add(Instructions.createOutput(Out_port)).build();    //创建一个treatment
                FlowRule newFLowRule = new DefaultFlowRule(curDevice, ts, tt, 10, new DefaultApplicationId(flowentry.appId(), "zlly"), 50000, false, flowentry.payLoad());
                flowRuleService.applyFlowRules(newFLowRule);
            }

        }
        //activeEntries = flowRuleService.getActiveFlowRuleCount(curDevice);
//            LOG.info("===准备安装的流规则: " + newFLowRule.toString()  );
//            LOG.info("===准备安装的流规则的appId:"+newFLowRule.appId() );
    }

    //zlzl=================================
    //搞定端口描述符号
    //监控信息可以拿到每条流的流速以及每条link的剩余带宽,还可以拿到每条link对应的交换机对
    //zlzlzl---２０１８．５．８ 当在此处理device事件时，会导致onos启动后无法检测到设备的变化
    //这里可以拿到具体的端口号(如果需要检测故障恢复，那我提前将之前已经关闭的端口先存到一个集合里面，之后在监控端口更改事件，然后查看该状态改变的端口是否在故障端口集合里面就行)
    //拿到故障主机的标识符和故障端口，将该主机上流表项的输出端口为故障端口的流表项失效
    /**
     *
     * 0.捕捉到发生了故障的交换机deviceId及其对应的端口号port go
     * 1.拿到该故障节点的所有流表项 go
     * 2.选出输出端口为故障端口的所有流表项 go
     * 3.令这些流表项失效 go
     * 4.根据流表项的id，拿到流id(按照代码中来看，流表项中的flowid，就是流id) go
     * 5.根据流id，拿到流速  go
     * 6.选出所有可用的候选路径 go
     * 7.根据每条流的流速，分配流的策略  go
     * 8.主动下发流表项 go  2018.5.30　基本完成
     *
     * 9.测出丢包率和故障恢复时延　do myself
     *      收到故障发生事件的一瞬间记录下一个时间点
     *      另一个...
     * 10.实现其他算法(胖树论文的算法)
     *
     */
    public void trafficDistributionStrategies(ConnectPoint srcConnectPoint, ConnectPoint dstConnectPoint)
    {

        //记录一些重要的变量
        List<FlowEntry> flowEntries = new ArrayList<FlowEntry>();       //flowentry集合,以故障端口为输出端口的所有流表项

        long sum_failLinkFlowRates = 0;                                 //故障link上的流速之和

        //故障交换机id
        //DeviceId deviceId = deviceId(uri(dpid));

        //通过交换机id和port形成一个connectionPoint
//        ConnectPoint connectPoint = ConnectPoint.deviceConnectPoint(deviceId.toString()+"/"+portDescription.portNumber().toString());

        //根据故障节点获取故障link对应的交换机
        DeviceId srcDeviceId = srcConnectPoint.deviceId();
        PortNumber srcPort = srcConnectPoint.port();

        //获取故障交换机故障端口的流速
        //map 用来存流id和对应的速率
        //Map<String, Double> flowIdRateMap = readFlowRateFile("/root/onos/statsOfLinks.txt");
        //Map<FlowId, Long> flowIdRateMap= getFlowRate(srcDeviceId, srcPort);
        //Map<FlowId, Double> flowIdRateMap= statisticService.getFlowIdRateMap(srcConnectPoint);      //要先点击界面
        Map<FlowId, Double> flowIdRateMap= statisticService.getFLowRate(srcConnectPoint);
        log.info("======与故障端口有关的flowIdRateMap形成完毕(从statistics直接传递)...flowIdRateMap.size:"+flowIdRateMap.size()+"flowIdMap:"+flowIdRateMap.keySet().toString());  //可用

//        for(FlowId flowId:flowIdRateMap.keySet())
//        {
//            if(flowIdRateMap.get(flowId)>0)
//            {
//                log.info("======流速大于零的流表项："+flowId.toString());
//            }
//        }

        //故障交换机的所有流表项
        Iterable<FlowEntry> activeEntries = flowRuleService.getFlowEntries(srcDeviceId);
        //log.info(srcDeviceId.toString() + "\n===设备" + srcDeviceId.toString() + "的流表项\n" + activeEntries);

        //遍历所有的流表项，找出输出端口为故障端口的流表项,并使这些流表项失效，同时还要要求优先级为10,不然待路径恢复之后，会影响其他link备份到这条故障link的流表项
        //虽然会进行流表项的恢复，但是如此庞大的恢复就没有必要了，因为我不做多条link故障的处理
        //如果需要做恢复，其实很简单，只需要将删除流表项之前将流表项存进一个集合，这样，故障之后，将这些流表项再次安装就行
        for(FlowEntry flowentry:activeEntries) {
            for (Instruction instruction : flowentry.treatment().immediate()) {
                if (instruction.type() == Instruction.Type.OUTPUT ) {
                    Instructions.OutputInstruction out = (Instructions.OutputInstruction) instruction;
                    //如果输出端口＝故障端口
                    if (out.port().equals(srcPort) && flowentry.priority()==10) {

                        log.info("======此交换机中含有输出端口为故障端口的流表项的id：" + flowentry.id());

                        //这里将与故障端口有关的流表项存储，便于后续计算分流策略
                        flowEntries.add(flowentry);

                        //拿到与该输出端口有关的所有流的id
                        try
                        {
                            sum_failLinkFlowRates += flowIdRateMap.get(flowentry.id());
                        }catch (NullPointerException e)
                        {
                            //life 被应用到交换机上已经存在了多长时间
                            log.info("============故障交换处没有与故障端口有关的流表项，flowIdRateMap为空<流id,流速>" + flowentry.id() + " 字节数 " + flowentry.bytes()+" 生存时间 "+flowentry.life());
                        }

                        //发生故障后，必须让主流表项失效，这样可以降低丢包率
                        log.info("============正在删除流表项...:" + flowentry.toString());
                        flowRuleService.removeFlowRules(flowentry);
                    }
                }
            }
        }

        //设定一个带宽阈值
        Double link_capacity = 10000000000.0;     //链路总容量,只能是９位数

        /**
         * 取得可用path(一共三条) ok
         * 取每条path的剩余带宽   ok
         * 取备份在这条path上的flowid  ok
         * 如果：
         *      每条备份路径的path都能保证有足够的带宽路由到该路径上的流量，就不进行处理　　ok
         * 否则：
         *      采用算法进行合适的匹配策略，进行流量分配     ok
         *      这里要充分兼顾已经下发的流表项，所以重点关注导致流量过大的path上的大流就可以了   ok
         *
         * 对于每条备用路径:
         *      剩余带宽信息 ok
         *      需要转移到这条link的流id和流速集合 ok
         *      是否流量爆满  ok
         *
         * 数据库操作：(可以用数据传递 flowIdPathMap)
         *      将流和备份路径存储到数据库表中
         *      需要定期的删除部分流表项（当某条流传输完成的时候，与之相应的流表项要删除）
         *      有可能会用到数据库中的锁操作
         *
         * 2018.5.31
         *      还差备份路径的激活问题   备份路径可以用，先不考虑这个，设置硬时间为10分钟先(验证备份路径流表项存在的有效性，然后解决flowIdPathMap传递)
         *      flowIdPathMap传递问题
         */

        //提前记录的<flowid,path>键值对（第一步可以仅仅做到存进流表项）
        //Map<FlowId, DefaultPath> flowIdPathMap = new HashMap<>();                      //流id，备份路径（需要提前查数据库存储或者通过其他方式传递）

        List<FlowEntry> flowEntry_About_path_List = new ArrayList<>();          //与某路径有关de流表项集合
        Map<Path, List<FlowEntry>> path_flowEntryList_Map = new HashMap<>();    //备份路径，流表项集合
        Map<Path, Double> path_remainCapacity_Map = new HashMap<>();            //备份路径，剩余容量
        Map<Path, Boolean> path_needTrafficDistribution_Map = new HashMap<>();  //备份路径，是否需要转移流量
        Map<Path, Double> pathSumRateMap = new HashMap<>();                     //备份路径，转移的流量总速率

        boolean needTrafficDistribution = false;

        //故障link两端交换机的可用路径
        /*
         * 拿到与某条路径有关的故障流表项的集合
         * 拿到备份路径的剩余带宽
         * 计算需要转移到该路径上的流量的总速率
         * 该路径是否需要进行分流操作
         */
        //端口更新在我的故障处理之后，所以此时的link还没有从拓扑中移除，因此不能用getPaths()
        Set<Path> anotherPaths = topologyService.getKShortestPaths(topologyService.currentTopology(), srcDeviceId, dstConnectPoint.deviceId(), DEFAULT_HOP_COUNT_WEIGHER, 4);
        //Set<Path> anotherPaths = topologyService.getPaths(topologyService.currentTopology(), srcDeviceId, dstConnectPoint.deviceId(), DEFAULT_HOP_COUNT_WEIGHER);
        log.info("======故障link两端交换机的可用路径计算完毕...anotherPaths.size:"+anotherPaths.size());
        for (Path path1 : anotherPaths)
        {
            if(path1.links().size()!=3)
            {
                continue;
            }
//            try
//            {
            //这里可以调用statisticManager里面的loadInternal，或许也可以传递map
            Link maxLink = statisticService.max(path1,"reactiveForwarding"); //已解决，用重载
            if(maxLink == null)
            {
                log.info("======path1上负载最大的link为空：maxLink=", maxLink);
            }
            Double remainBandwith = link_capacity - statisticService.load(maxLink).rate()*1.0;    //该路径的剩余带宽,每一个交换机上的flowid不一样
            pathSumRateMap.put(path1, 0*0.1);
            log.info("============在分流策略中获取到的flowIdPathMap.size():"+flowIdPathMap.size());  //可以拿到

            for (FlowEntry flowEntry: flowEntries)
            {
                if(flowIdPathMap.get(flowEntry.id()).links().equals(path1.links()))     //可能要改动,改为links可以用
                {
                    log.info("======备份路径的links前后对比...(已进入),  flowIdRateMap.get(key):");
                    boolean flag = flowIdRateMap.containsKey(flowEntry.id());
                    if(flag == true)
                    {
                        pathSumRateMap.put(path1, pathSumRateMap.get(path1)+flowIdRateMap.get(flowEntry.id()));   //形成了path和要转移的流量总量的键值对
                    }
                    else
                    {
                        log.info("flowIdRateMap中不存在该键...");
                    }
                    flowEntry_About_path_List.add(flowEntry);
                }
            }
            path_flowEntryList_Map.put(path1, flowEntry_About_path_List);

            log.info("======要转移到该path的流量总量："+pathSumRateMap.get(path1) +" 该path上的剩余带宽：" + remainBandwith*1.0);
            //是否流量爆满？需要进行分流操作
            if(pathSumRateMap.get(path1) > remainBandwith*1.0)
            {
                needTrafficDistribution = true;
                path_needTrafficDistribution_Map.put(path1, true);
            }else
            {
                path_needTrafficDistribution_Map.put(path1, false);
            }

            //剩余带宽信息
            //Double remain_capacity = link_capacity - statisticService.load(statisticService.max(path1)).rate();
            path_remainCapacity_Map.put(path1, remainBandwith);
//            }
//            catch (NullPointerException e)
//            {
//                log.info("==============目前没有流经过，没有与故障link有关的流表项");
//            }
        }

        /**
         * 先为每条流制定备份路径(即每流每link备份路径，故障后主路径流表项失效，流量会直接就流量路由到事先备份的路径)
         * 多条流备份到同一条路径上，流量过大时，依然需要分流
         * 分流策略：
         *      选出该路径上的最大流，看另外两条路径是否可以路由，可以则进行路由，不可以则对次大流进行路由，直到该路径的带宽可以路由剩余流量，或者流集合遍历完毕（后期可改为大流集合,可能还要对需要遍历的流表项进行限制）
         *      遍历剩余的路径
         */
        log.info("======根据被中断后的流量的流速大小，以及备用路径的剩余容量进行判断，是否进行分流操作...");
        if( needTrafficDistribution == true)    //需要进行流量调整
        {
            log.info("======开始进行分流操作...");
            for(Path path: anotherPaths){
                if (path_needTrafficDistribution_Map.get(path) == false)  //过滤掉不需要进行流量调整的path
                {
                    continue;
                }
                while(path_remainCapacity_Map.get(path) - pathSumRateMap.get(path) < 0)
                {
                    //拿到该流量过载链路上的最大流
                    FlowEntry maxflowentry = path_flowEntryList_Map.get(path).get(0);
                    for(FlowEntry flowEntry: path_flowEntryList_Map.get(path))
                    {
                        if(flowIdRateMap.get(flowEntry.id()) > flowIdRateMap.get(maxflowentry.id()))
                        {
                            maxflowentry = flowEntry;
                        }
                    }

                    //找到路由最大流的路径
                    for(Path path1: anotherPaths)
                    {
                        if(path1 != path)
                        {
                            if(path_remainCapacity_Map.get(path1)-pathSumRateMap.get(path1)>flowIdRateMap.get(maxflowentry.id()))
                            {
                                //新选定的路径对应的总流量增加
                                pathSumRateMap.put(path1, pathSumRateMap.get(path1) + flowIdRateMap.get(maxflowentry.id()));
                                //原来的路径对应的总流量减少
                                pathSumRateMap.put(path, pathSumRateMap.get(path) - flowIdRateMap.get(maxflowentry.id()));

                                //主动下发流表项
                                installRuleInActive(maxflowentry, path1);        //流表项
                            }
                        }
                    }
                    path_flowEntryList_Map.get(path).remove(maxflowentry);     //这条最大流不可用或已经转移出去，继续遍历这条路径时都不在考虑
                }
            }
        }
    }


    // Indicates whether this is a control packet, e.g. LLDP, BDDP，判断数据包是否是一个控制数据包
    private boolean isControlPacket(Ethernet eth) {
        short type = eth.getEtherType();
        return type == Ethernet.TYPE_LLDP || type == Ethernet.TYPE_BSN;
    }

    // Indicated whether this is an IPv6 multicast packet.判断是否是IP6广播数据包
    private boolean isIpv6Multicast(Ethernet eth) {
        return eth.getEtherType() == Ethernet.TYPE_IPV6 && eth.isMulticast();
    }

    // Selects a path from the given set that does not lead back to the
    // specified port if possible.如果可能的话，从给定集合中选择一条不返回指定端口的路径。
    private Path pickForwardPathIfPossible(Set<Path> paths, PortNumber notToPort) {
        for (Path path : paths) {
            if (!path.src().port().equals(notToPort)) {
                return path;
            }
        }
        return null;
    }

    // Floods the specified packet if permissible.如果允许的话，对该数据包泛洪
    private void flood(PacketContext context, ReactiveForwardMetrics macMetrics) {
        if (topologyService.isBroadcastPoint(topologyService.currentTopology(),
                                             context.inPacket().receivedFrom())) {
            packetOut(context, PortNumber.FLOOD, macMetrics);
        } else {
            context.block();
        }
    }

    // Sends a packet out the specified port.从指定的端口发送数据包,由数据包的上下文发送数据包
    private void packetOut(PacketContext context, PortNumber portNumber, ReactiveForwardMetrics macMetrics) {
        replyPacket(macMetrics);//只是一个简答的计数
        context.treatmentBuilder().setOutput(portNumber);
        context.send();
    }

    // Install a rule forwarding the packet to the specified port.
    // 安装规则将数据包转发到指定端口

    /**.
     * 这个函数的主要功能如下：
     *      根据数据包是否是packetOut数据包，判断是否进行直接转发操作
     *      根据数据包里面的字段建立selectorBuilder(流量选择器，用来匹配数据包的各种字段，例如：matchInPort,matchIPSrc等)
     *      根据 portNumber 建立 treatment(用来说明怎么处理包，丢弃还是转发，从哪儿转发，包含IN_PORT的消息)
     *      指定流表规则，比较重要的部分有两个：
     *      按需指定流表规则，并发出，很自动化
     *selectorBuilder：
     *      看传进来的packet_in数据包对各个字段的匹配情况
     *
     *  如果是大流的备用路径，必须要进行精确匹配．小流最好进行通配符匹配操作，但是通配符匹配的备用路径．似乎无法实现
     *  先试着进行精确匹配，如果通配符的操作无法实现还可以实施故障后的分流
     *
     *
     *  精确匹配需要context.inPacket()来提供各种信息，那么各种匹配字段都不需要改动，只需要修改优先级
     *
     * @param context
     * @param portNumber
     * @param macMetrics
     */
    // Install a rule forwarding the packet to the specified port.安装一条流转发数据包到指定的端口
    private void installRule(PacketContext context, PortNumber portNumber, ReactiveForwardMetrics macMetrics) {
        //
        // We don't support (yet) buffer IDs in the Flow Service so
        // packet out first.我们先在还不支持流服务的缓存ID，所以，先转发出去
        //
        Ethernet inPkt = context.inPacket().parsed();
        TrafficSelector.Builder selectorBuilder = DefaultTrafficSelector.builder();

        // If PacketOutOnly or ARP packet than forward directly to output port
        // 如果是PacketOutOnly或者ARP数据包，直接转发到输出端口
        if (packetOutOnly || inPkt.getEtherType() == Ethernet.TYPE_ARP) {
            packetOut(context, portNumber, macMetrics);
            return;
        }

        //
        // If matchDstMacOnly
        //    Create flows matching dstMac only
        // Else
        //    Create flows with default matching and include configured fields
        //
        if (matchDstMacOnly) {
            selectorBuilder.matchEthDst(inPkt.getDestinationMAC());
        } else {
            selectorBuilder.matchInPort(context.inPacket().receivedFrom().port())
                    .matchEthSrc(inPkt.getSourceMAC())
                    .matchEthDst(inPkt.getDestinationMAC());

            // If configured Match Vlan ID
            if (matchVlanId && inPkt.getVlanID() != Ethernet.VLAN_UNTAGGED) {
                selectorBuilder.matchVlanId(VlanId.vlanId(inPkt.getVlanID()));
            }

            //
            // If configured and EtherType is IPv4 - Match IPv4 and
            // TCP/UDP/ICMP fields
            //
            if (matchIpv4Address && inPkt.getEtherType() == Ethernet.TYPE_IPV4) {
                IPv4 ipv4Packet = (IPv4) inPkt.getPayload();
                byte ipv4Protocol = ipv4Packet.getProtocol();
                Ip4Prefix matchIp4SrcPrefix =
                        Ip4Prefix.valueOf(ipv4Packet.getSourceAddress(),
                                          Ip4Prefix.MAX_MASK_LENGTH);
                Ip4Prefix matchIp4DstPrefix =
                        Ip4Prefix.valueOf(ipv4Packet.getDestinationAddress(),
                                          Ip4Prefix.MAX_MASK_LENGTH);
                selectorBuilder.matchEthType(Ethernet.TYPE_IPV4)
                        .matchIPSrc(matchIp4SrcPrefix)
                        .matchIPDst(matchIp4DstPrefix);

                if (matchIpv4Dscp) {
                    byte dscp = ipv4Packet.getDscp();
                    byte ecn = ipv4Packet.getEcn();
                    selectorBuilder.matchIPDscp(dscp).matchIPEcn(ecn);
                }

                if (matchTcpUdpPorts && ipv4Protocol == IPv4.PROTOCOL_TCP) {
                    TCP tcpPacket = (TCP) ipv4Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv4Protocol)
                            .matchTcpSrc(TpPort.tpPort(tcpPacket.getSourcePort()))
                            .matchTcpDst(TpPort.tpPort(tcpPacket.getDestinationPort()));
                }
                if (matchTcpUdpPorts && ipv4Protocol == IPv4.PROTOCOL_UDP) {
                    UDP udpPacket = (UDP) ipv4Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv4Protocol)
                            .matchUdpSrc(TpPort.tpPort(udpPacket.getSourcePort()))
                            .matchUdpDst(TpPort.tpPort(udpPacket.getDestinationPort()));
                }
                if (matchIcmpFields && ipv4Protocol == IPv4.PROTOCOL_ICMP) {
                    ICMP icmpPacket = (ICMP) ipv4Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv4Protocol)
                            .matchIcmpType(icmpPacket.getIcmpType())
                            .matchIcmpCode(icmpPacket.getIcmpCode());
                }
            }

            //
            // If configured and EtherType is IPv6 - Match IPv6 and
            // TCP/UDP/ICMP fields
            //
            if (matchIpv6Address && inPkt.getEtherType() == Ethernet.TYPE_IPV6) {
                IPv6 ipv6Packet = (IPv6) inPkt.getPayload();
                byte ipv6NextHeader = ipv6Packet.getNextHeader();
                Ip6Prefix matchIp6SrcPrefix =
                        Ip6Prefix.valueOf(ipv6Packet.getSourceAddress(),
                                          Ip6Prefix.MAX_MASK_LENGTH);
                Ip6Prefix matchIp6DstPrefix =
                        Ip6Prefix.valueOf(ipv6Packet.getDestinationAddress(),
                                          Ip6Prefix.MAX_MASK_LENGTH);
                selectorBuilder.matchEthType(Ethernet.TYPE_IPV6)
                        .matchIPv6Src(matchIp6SrcPrefix)
                        .matchIPv6Dst(matchIp6DstPrefix);

                if (matchIpv6FlowLabel) {
                    selectorBuilder.matchIPv6FlowLabel(ipv6Packet.getFlowLabel());
                }

                if (matchTcpUdpPorts && ipv6NextHeader == IPv6.PROTOCOL_TCP) {
                    TCP tcpPacket = (TCP) ipv6Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv6NextHeader)
                            .matchTcpSrc(TpPort.tpPort(tcpPacket.getSourcePort()))
                            .matchTcpDst(TpPort.tpPort(tcpPacket.getDestinationPort()));
                }
                if (matchTcpUdpPorts && ipv6NextHeader == IPv6.PROTOCOL_UDP) {
                    UDP udpPacket = (UDP) ipv6Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv6NextHeader)
                            .matchUdpSrc(TpPort.tpPort(udpPacket.getSourcePort()))
                            .matchUdpDst(TpPort.tpPort(udpPacket.getDestinationPort()));
                }
                if (matchIcmpFields && ipv6NextHeader == IPv6.PROTOCOL_ICMP6) {
                    ICMP6 icmp6Packet = (ICMP6) ipv6Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv6NextHeader)
                            .matchIcmpv6Type(icmp6Packet.getIcmpType())
                            .matchIcmpv6Code(icmp6Packet.getIcmpCode());
                }
            }
        }

        //流量处理器，负责添加流表中的指令
        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                .setOutput(portNumber)
                .build();

//        //构建流规则对象，输入流量处理器treatement，selectorBuilder,优先级，appid,流的持续时间
//        ForwardingObjective forwardingObjective = DefaultForwardingObjective.builder()
//                .withSelector(selectorBuilder.build())
//                .withTreatment(treatment)
//                .withPriority(flowPriority)
//                .withFlag(ForwardingObjective.Flag.VERSATILE)
//                .fromApp(appId)
//                .makeTemporary(flowTimeout)
//                .add();
//
//        //通过流对象服务，转发出转发对象。在指定的设备上安装流规则
//        flowObjectiveService.forward(context.inPacket().receivedFrom().deviceId(),
//                                     forwardingObjective);

        FlowRule.Builder flowRuleBuilder = DefaultFlowRule.builder()
                .forDevice(context.inPacket().receivedFrom().deviceId())
                .withSelector(selectorBuilder.build())
                .withTreatment(treatment)
                .withPriority(flowPriority)
                .fromApp(appId)
                .makeTemporary(10);
//                .withHardTimeout(300);      //硬超时时间

        FlowRuleOperations.Builder flowOpsBuilder = FlowRuleOperations.builder();
        flowOpsBuilder = flowOpsBuilder.add(flowRuleBuilder.build());

        //这里提前定义一个全局变量用来获取改变后的值
        flowId_installRule = flowRuleBuilder.build().id();

        flowRuleService.apply(flowOpsBuilder.build(new FlowRuleOperationsContext() {
            @Override
            public void onSuccess(FlowRuleOperations ops) {
                log.debug("FlowRule安装成功");
            }

            @Override
            public void onError(FlowRuleOperations ops) {
                log.debug("Failed to privision vni or forwarding table");
            }
        }));


        forwardPacket(macMetrics);//增加转发数据包的计数
        //
        // If packetOutOfppTable
        //  Send packet back to the OpenFlow pipeline to match installed flow
        // Else
        //  Send packet direction on the appropriate port
        //如果packetOutOfppTable为真，那么将数据包转发到交换机的TABLE端口，流水线的开始。
        if (packetOutOfppTable) {
            packetOut(context, PortNumber.TABLE, macMetrics);
        } else {
            packetOut(context, portNumber, macMetrics);
        }
    }

    //备用路径的优先级暂时设定为5
    //五元组需要精确匹配
    //现在问题在于下发路径的机制，一次只安装一个流表项，又要再次发送请求，继续请求从下一个节点到目的节点的，而不是我指定的路径的
    //现在还不好检测备份路径是否成功设置，还需检测link故障事件，然后压制onos自己的处理，然后让流表项失效，应该就可以从界面(不用压制，有流表项之后就可以了)
    private void installBackupRule(PacketContext context, PortNumber portNumber, PortNumber In_port,  DeviceId srcLinkDeviceId, int backupFlowPriority) {
        //
        // We don't support (yet) buffer IDs in  the Flow Service so
        // packet out first.
        //
        //log.info("=======================installBackupRule============================");
//        log.info("==================参数srcLinkDeviceId:"+ srcLinkDeviceId +" ======");
//        log.info("==================参数portNumer的值为:"+ portNumber +" ======");
        Ethernet inPkt = context.inPacket().parsed();       //解析packetin包
//        System.out.println("进入了installBackupRule函数的inPkt数据包的内容：" + inPkt.toString());

        //与packet_in里面带来的各种字段的值一一对应，形成匹配项
        TrafficSelector.Builder selectorBuilder = DefaultTrafficSelector.builder();

        // If PacketOutOnly or ARP packet than forward directly to output port
        //上面有一个packetOutOnly赋值
//        if (packetOutOnly || inPkt.getEtherType() == Ethernet.TYPE_ARP) {       //pktOut或者arp包，直接转到输出端口
//            packetOut(context, portNumber, macMetrics);
//            return;
//        }

        //
        // If matchDstMacOnly
        //    Create flows matching dstMac only
        // Else
        //    Create flows with default matching and include configured fields
        //
        if (matchDstMacOnly) {
            //仅仅匹配目的地址，那其他的就不用匹配了
            selectorBuilder.matchEthDst(inPkt.getDestinationMAC());
        } else {
            selectorBuilder.matchInPort(In_port)     //匹配输入端口
                    .matchEthSrc(inPkt.getSourceMAC())
                    .matchEthDst(inPkt.getDestinationMAC());        //用到了入端口，源目mac

            // If configured Match Vlan ID
            if (matchVlanId && inPkt.getVlanID() != Ethernet.VLAN_UNTAGGED) {
                selectorBuilder.matchVlanId(VlanId.vlanId(inPkt.getVlanID()));
            }

            //
            // If configured and EtherType is IPv4 - Match IPv4 and
            // TCP/UDP/ICMP fields
            //
            if (matchIpv4Address && inPkt.getEtherType() == Ethernet.TYPE_IPV4) {
                IPv4 ipv4Packet = (IPv4) inPkt.getPayload();
                byte ipv4Protocol = ipv4Packet.getProtocol();
                Ip4Prefix matchIp4SrcPrefix =
                        Ip4Prefix.valueOf(ipv4Packet.getSourceAddress(),
                                Ip4Prefix.MAX_MASK_LENGTH);
                Ip4Prefix matchIp4DstPrefix =
                        Ip4Prefix.valueOf(ipv4Packet.getDestinationAddress(),
                                Ip4Prefix.MAX_MASK_LENGTH);
                selectorBuilder.matchEthType(Ethernet.TYPE_IPV4)
                        .matchIPSrc(matchIp4SrcPrefix)
                        .matchIPDst(matchIp4DstPrefix);

                if (matchIpv4Dscp) {
                    byte dscp = ipv4Packet.getDscp();
                    byte ecn = ipv4Packet.getEcn();
                    selectorBuilder.matchIPDscp(dscp).matchIPEcn(ecn);
                }

                if (matchTcpUdpPorts && ipv4Protocol == IPv4.PROTOCOL_TCP) {
                    TCP tcpPacket = (TCP) ipv4Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv4Protocol)
                            .matchTcpSrc(TpPort.tpPort(tcpPacket.getSourcePort()))
                            .matchTcpDst(TpPort.tpPort(tcpPacket.getDestinationPort()));
                }
                if (matchTcpUdpPorts && ipv4Protocol == IPv4.PROTOCOL_UDP) {
                    UDP udpPacket = (UDP) ipv4Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv4Protocol)
                            .matchUdpSrc(TpPort.tpPort(udpPacket.getSourcePort()))
                            .matchUdpDst(TpPort.tpPort(udpPacket.getDestinationPort()));
                }
                if (matchIcmpFields && ipv4Protocol == IPv4.PROTOCOL_ICMP) {
                    ICMP icmpPacket = (ICMP) ipv4Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv4Protocol)
                            .matchIcmpType(icmpPacket.getIcmpType())
                            .matchIcmpCode(icmpPacket.getIcmpCode());
                }
            }

            //
            // If configured and EtherType is IPv6 - Match IPv6 and
            // TCP/UDP/ICMP fields
            //
            if (matchIpv6Address && inPkt.getEtherType() == Ethernet.TYPE_IPV6) {
                IPv6 ipv6Packet = (IPv6) inPkt.getPayload();
                byte ipv6NextHeader = ipv6Packet.getNextHeader();
                Ip6Prefix matchIp6SrcPrefix =
                        Ip6Prefix.valueOf(ipv6Packet.getSourceAddress(),
                                Ip6Prefix.MAX_MASK_LENGTH);
                Ip6Prefix matchIp6DstPrefix =
                        Ip6Prefix.valueOf(ipv6Packet.getDestinationAddress(),
                                Ip6Prefix.MAX_MASK_LENGTH);
                selectorBuilder.matchEthType(Ethernet.TYPE_IPV6)
                        .matchIPv6Src(matchIp6SrcPrefix)
                        .matchIPv6Dst(matchIp6DstPrefix);

                if (matchIpv6FlowLabel) {
                    selectorBuilder.matchIPv6FlowLabel(ipv6Packet.getFlowLabel());
                }

                if (matchTcpUdpPorts && ipv6NextHeader == IPv6.PROTOCOL_TCP) {
                    TCP tcpPacket = (TCP) ipv6Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv6NextHeader)
                            .matchTcpSrc(TpPort.tpPort(tcpPacket.getSourcePort()))
                            .matchTcpDst(TpPort.tpPort(tcpPacket.getDestinationPort()));
                }
                if (matchTcpUdpPorts && ipv6NextHeader == IPv6.PROTOCOL_UDP) {
                    UDP udpPacket = (UDP) ipv6Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv6NextHeader)
                            .matchUdpSrc(TpPort.tpPort(udpPacket.getSourcePort()))
                            .matchUdpDst(TpPort.tpPort(udpPacket.getDestinationPort()));
                }
                if (matchIcmpFields && ipv6NextHeader == IPv6.PROTOCOL_ICMP6) {
                    ICMP6 icmp6Packet = (ICMP6) ipv6Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv6NextHeader)
                            .matchIcmpv6Type(icmp6Packet.getIcmpType())
                            .matchIcmpv6Code(icmp6Packet.getIcmpCode());
                }
            }
        }
        //selectorBuilder借用inPacket的一些值设置头部匹配字段

        //treatment，用来设置怎么处理流，丢弃还是转发，从哪儿转发，包含IN_PORT的消息
        //这里指的是该路径的源地址的port()，这个有什么意义?
        //因为每一次下发的流表项都是只作用于发送packet_in的源交换机，所以这里很有可能是指定输出端口
        //没错,treatment就是匹完之后采取的动作，portNumber即是表示指定输出端口
        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                .setOutput(portNumber)
                .build();

        //selector 用来设置流表项中用来匹配流各种字段的值：源目的ip,源目的mac，源目的port等等
        //treatment 用来设置匹配后的动作：设置输出端口，转发，丢弃等等
        //
        //指定备用路径重点是修改　forwardingObjective.
        //需要指定备用路径的优先级难道要进行重载？
        //
//        ForwardingObjective forwardingObjective = DefaultForwardingObjective.builder()
//                .withSelector(selectorBuilder.build())
//                .withTreatment(treatment)
//                .withPriority(8)   //backupFlowPriority
//                .withFlag(ForwardingObjective.Flag.VERSATILE)
//                .fromApp(appId)
//                .makeTemporary(20)
//                .add();
//
//        //可能是指该数据包将要到达的下一个设备
//        //仅仅给该路径的第一台设备安装了转发规则
//        //安装转发规则到特定的设备
//        //修改这里，可以制定安装流表项的设备
//        flowObjectiveService.forward(srcLinkDeviceId, forwardingObjective);

        //采用l的方式下发备用路径，可以设置硬超时时间(注意设置的forDevice)
        FlowRule.Builder flowRuleBuilder = DefaultFlowRule.builder()
                .forDevice(srcLinkDeviceId)
                .withSelector(selectorBuilder.build())
                .withTreatment(treatment)
                .withPriority(backupFlowPriority)
                .fromApp(appId)
                .withHardTimeout(60);      //硬超时时间

        FlowRuleOperations.Builder flowOpsBuilder = FlowRuleOperations.builder();
        flowOpsBuilder = flowOpsBuilder.add(flowRuleBuilder.build());

        flowRuleService.apply(flowOpsBuilder.build(new FlowRuleOperationsContext() {
            @Override
            public void onSuccess(FlowRuleOperations ops) {
                log.debug("FlowRule安装成功");
            }

            @Override
            public void onError(FlowRuleOperations ops) {
                log.debug("Failed to privision vni or forwarding table");
            }
        }));

        //我是设置备用路径，不需要这一步
//        forwardPacket(macMetrics);

        //zlzl此时再次打印流表项的数量，看是否发生了变化
        //仅仅只是安装了该路径的第一台交换机,
//        log.info("\n===进入install函数，安装了规则的设备：" + context.inPacket().receivedFrom().deviceId().toString());
//        long activeEntries = flowRuleService.getActiveFlowRuleCount(context.inPacket().receivedFrom().deviceId());
//        log.info("\n===设备"+context.inPacket().receivedFrom().deviceId().toString()+"的流表项数量:( " + activeEntries + " )");
//        log.info("\n===给该设备安装的规则是:" + forwardingObjective.toString());

        //
        // If packetOutOfppTable
        //  Send packet back to the OpenFlow pipeline to match installed flow
        // Else
        //  Send packet direction on the appropriate port
        //  控制器向数据平面的交换机发送消息就是Packet_out消息
//        if (packetOutOfppTable) {
//            packetOut(context, PortNumber.TABLE, macMetrics);
//        } else {
//            packetOut(context, portNumber, macMetrics);
//        }
    }


    //全局变量，保存故障发生时的link,两条，方向相反
    List<Link> links = new ArrayList<>();
    Link failLlink = null;

    //仿写一个监听，然后进行处理
    private class InternalLinkListener implements LinkListener
    {
        @Override
        public void event(LinkEvent event) {
            if(event.type().equals(LinkEvent.Type.LINK_REMOVED))
            {
                log.info("====================link失效:"+event.toString());
                links.add(event.subject());
                //先测单向
                //            failLlink = links.get(0);
                failLlink = event.subject();
                log.info("======failLink:"+failLlink.src().deviceId().toString());
                if(failLlink.src().deviceId().toString().equals("of:0000002000000000"))   //这样就只会调用一次,字符串比较用equals
                {
                    log.info("======进入分流策略...");
                    log.info("======failLink:"+failLlink.toString());
                    trafficDistributionStrategies( failLlink.src(), failLlink.dst());
                }
            }
        }
    }

    //拓扑监听器的实现类
    private class InternalTopologyListener implements TopologyListener {
        @Override
        public void event(TopologyEvent event) {
            List<Event> reasons = event.reasons();
            if (reasons != null) {
                reasons.forEach(re -> {
                    if (re instanceof LinkEvent) {
                        LinkEvent le = (LinkEvent) re;
                        //如果出现了链路删除并且黑洞执行不是空，那么
                        if (le.type() == LinkEvent.Type.LINK_REMOVED && blackHoleExecutor != null) {
                            log.info("=======进入onos自带的故障处理机制...." + event.type());
                            //blackHoleExecutor.submit(() -> fixBlackhole(le.subject().src()));
                        }
                    }
                });
            }
        }
    }

    //修复黑洞？？
    private void fixBlackhole(ConnectPoint egress) {
        Set<FlowEntry> rules = getFlowRulesFrom(egress);//从指定的点获得所有的流规则
        Set<SrcDstPair> pairs = findSrcDstPairs(rules);

        Map<DeviceId, Set<Path>> srcPaths = new HashMap<>();

        for (SrcDstPair sd : pairs) {
            // get the edge deviceID for the src host
            Host srcHost = hostService.getHost(HostId.hostId(sd.src));
            Host dstHost = hostService.getHost(HostId.hostId(sd.dst));
            if (srcHost != null && dstHost != null) {
                DeviceId srcId = srcHost.location().deviceId();
                DeviceId dstId = dstHost.location().deviceId();
                log.trace("SRC ID is {}, DST ID is {}", srcId, dstId);

                cleanFlowRules(sd, egress.deviceId());

                Set<Path> shortestPaths = srcPaths.get(srcId);
                if (shortestPaths == null) {
                    shortestPaths = topologyService.getPaths(topologyService.currentTopology(),
                            egress.deviceId(), srcId);
                    srcPaths.put(srcId, shortestPaths);
                }
                backTrackBadNodes(shortestPaths, dstId, sd);
            }
        }
    }

    // Backtracks from link down event to remove flows that lead to blackhole
    //进行回溯，移除最短路径上与故障节点对有关的所有的流表项

    /**
     * @param shortestPaths
     * @param dstId
     * @param sd
     * 遍历每一条最短路径，拿到该路径上的每一条link
     * 取每一条link的源交换机，不包括第一条link，删除该交换机上源目主机对为故障主机对的流表项
     * 找一条从当前交换机到目的交换机的路径，保证不经过指定端口
     */
    // Backtracks from link down event to remove flows that lead to blackhole
    private void backTrackBadNodes(Set<Path> shortestPaths, DeviceId dstId, SrcDstPair sd) {
        for (Path p : shortestPaths) {
            List<Link> pathLinks = p.links();
            for (int i = 0; i < pathLinks.size(); i = i + 1) {
                Link curLink = pathLinks.get(i);
                DeviceId curDevice = curLink.src().deviceId();

                // skipping the first link because this link's src has already been pruned beforehand
                if (i != 0) {
                    cleanFlowRules(sd, curDevice);
                }

                Set<Path> pathsFromCurDevice =
                        topologyService.getPaths(topologyService.currentTopology(),
                                                 curDevice, dstId);
                if (pickForwardPathIfPossible(pathsFromCurDevice, curLink.src().port()) != null) {
                    break;
                } else {
                    if (i + 1 == pathLinks.size()) {
                        cleanFlowRules(sd, curLink.dst().deviceId());
                    }
                }
            }
        }
    }

    // Removes flow rules off specified device with specific SrcDstPair
    // 删除指定设备上特定源目的结点对的流规则，参数：源目的结点对，设备ｉｄ
    /*
     * 先拿到故障交换机的所有流表项
     * 找到输出端口
     * 找到特定源目结点对的流表项
     * 移除该规则（怪不得会把我的主备用路径都移除了，但是没有看到判断端口的操作，如果有，就和我的契合了）
     */
    // Removes flow rules off specified device with specific SrcDstPair
    private void cleanFlowRules(SrcDstPair pair, DeviceId id) {
        log.trace("Searching for flow rules to remove from: {}", id);
        log.trace("Removing flows w/ SRC={}, DST={}", pair.src, pair.dst);
        log.info("===============这里是onos自带的删除故障端口流表项的操作：Searching for flow rules to remove from: {}", id);
        log.info("===============这里是onos自带的删除故障端口流表项的操作：Removing flows w/ SRC={}, DST={}", pair.src, pair.dst);

        for (FlowEntry r : flowRuleService.getFlowEntries(id)) {
            boolean matchesSrc = false, matchesDst = false;
            for (Instruction i : r.treatment().allInstructions()) {
                if (i.type() == Instruction.Type.OUTPUT) {
                    // if the flow has matching src and dst
                    for (Criterion cr : r.selector().criteria()) {
                        if (cr.type() == Criterion.Type.ETH_DST) {
                            if (((EthCriterion) cr).mac().equals(pair.dst)) {
                                matchesDst = true;
                            }
                        } else if (cr.type() == Criterion.Type.ETH_SRC) {
                            if (((EthCriterion) cr).mac().equals(pair.src)) {
                                matchesSrc = true;
                            }
                        }
                    }
                }
            }
            if (matchesDst && matchesSrc) {
                log.info("======================onos自带的删除流表项的操作Removed flow rule from device: {} , {}", id, r.toString());
                log.trace("Removed flow rule from device: {}", id);
                flowRuleService.removeFlowRules((FlowRule) r);
            }
        }

    }

    // Returns a set of src/dst MAC pairs extracted from the specified set of flow entries
    // 从规则中提取出源目的主机对
    // Returns a set of src/dst MAC pairs extracted from the specified set of flow entries
    //从指定的流表想中找到所有的src/dst mac地址对
    private Set<SrcDstPair> findSrcDstPairs(Set<FlowEntry> rules) {
        ImmutableSet.Builder<SrcDstPair> builder = ImmutableSet.builder();
        for (FlowEntry r : rules) {
            MacAddress src = null, dst = null;
            for (Criterion cr : r.selector().criteria()) {
                if (cr.type() == Criterion.Type.ETH_DST) {
                    dst = ((EthCriterion) cr).mac();
                } else if (cr.type() == Criterion.Type.ETH_SRC) {
                    src = ((EthCriterion) cr).mac();
                }
            }
            builder.add(new SrcDstPair(src, dst));
        }
        return builder.build();
    }

    //创造计数器
    private ReactiveForwardMetrics createCounter(MacAddress macAddress) {
        ReactiveForwardMetrics macMetrics = null;
        if (recordMetrics) {
            macMetrics = metrics.compute(macAddress, (key, existingValue) -> {
                if (existingValue == null) {
                    return new ReactiveForwardMetrics(0L, 0L, 0L, 0L, macAddress);
                } else {
                    return existingValue;
                }
            });
        }
        return macMetrics;
    }

    //增加转发计数
    private void  forwardPacket(ReactiveForwardMetrics macmetrics) {
        if (recordMetrics) {
            macmetrics.incrementForwardedPacket();
            metrics.put(macmetrics.getMacAddress(), macmetrics);
        }
    }

    //增加收到的数据包的计数
    private void inPacket(ReactiveForwardMetrics macmetrics) {
        if (recordMetrics) {
            macmetrics.incrementInPacket();
            metrics.put(macmetrics.getMacAddress(), macmetrics);
        }
    }

    //增加回复的数据包的计数
    private void replyPacket(ReactiveForwardMetrics macmetrics) {
        if (recordMetrics) {
            macmetrics.incremnetReplyPacket();//增加返回数据包的计数
            metrics.put(macmetrics.getMacAddress(), macmetrics);
        }
    }

    //增加丢弃数据包的计数
    private void droppedPacket(ReactiveForwardMetrics macmetrics) {
        if (recordMetrics) {
            macmetrics.incrementDroppedPacket();
            metrics.put(macmetrics.getMacAddress(), macmetrics);
        }
    }

    public EventuallyConsistentMap<MacAddress, ReactiveForwardMetrics> getMacAddress() {
        return metrics;
    }

    public void printMetric(MacAddress mac) {
        System.out.println("-----------------------------------------------------------------------------------------");
        System.out.println(" MACADDRESS \t\t\t\t\t\t Metrics");
        if (mac != null) {
            System.out.println(" " + mac + " \t\t\t " + metrics.get(mac));
        } else {
            for (MacAddress key : metrics.keySet()) {
                System.out.println(" " + key + " \t\t\t " + metrics.get(key));
            }
        }
    }

    //从指定的链接点获得所有的流规则
    private Set<FlowEntry> getFlowRulesFrom(ConnectPoint egress) {
        ImmutableSet.Builder<FlowEntry> builder = ImmutableSet.builder();
        flowRuleService.getFlowEntries(egress.deviceId()).forEach(r -> {
            if (r.appId() == appId.id()) {
                r.treatment().allInstructions().forEach(i -> {
                    if (i.type() == Instruction.Type.OUTPUT) {
                        if (((Instructions.OutputInstruction) i).port().equals(egress.port())) {
                            builder.add(r);
                        }
                    }
                });
            }
        });

        return builder.build();
    }

    // Wrapper class for a source and destination pair of MAC addresses
    //源和目的MAC地址的包装类
    private final class SrcDstPair {
        final MacAddress src;
        final MacAddress dst;

        private SrcDstPair(MacAddress src, MacAddress dst) {
            this.src = src;
            this.dst = dst;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SrcDstPair that = (SrcDstPair) o;
            return Objects.equals(src, that.src) &&
                    Objects.equals(dst, that.dst);
        }

        @Override
        public int hashCode() {
            return Objects.hash(src, dst);
        }
    }
}
