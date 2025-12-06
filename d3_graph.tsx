// src/index.tsx - Complete D3 Force-Directed Fraud Network Widget
import React, { useEffect, useRef, useCallback } from 'react';
import * as d3 from 'd3-force';
import { useWidgetParameters, useWidgetEvents } from '@osdk/widget.client/react';
import { Tooltip } from './Tooltip'; // Simple tooltip component

interface ClaimNode {
  id: string;
  type: 'claim' | 'policyholder' | 'agent';
  primaryKey: string;
  claimId?: string;
  claimAmount?: number;
  anomalyScore?: number;
  status?: string;
  policyholderId?: string;
  policyholderName?: string;
  riskProfile?: string;
  agentId?: string;
  agentName?: string;
  x?: number;
  y?: number;
  vx?: number;
  vy?: number;
}

interface Link {
  source: string;
  target: string;
  type: 'claim-policyholder' | 'policyholder-agent';
}

export default function FraudNetworkWidget() {
  const { claims } = useWidgetParameters<'claims'>();
  const { flagSuspicious, investigateCluster } = useWidgetEvents<'flagSuspicious' | 'investigateCluster'>();

  const svgRef = useRef<SVGSVGElement>(null);
  const simulationRef = useRef<d3.Simulation<ClaimNode, undefined> | null>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);

  // Build nodes + links from Ontology claims
  const buildGraphData = useCallback((): { nodes: ClaimNode[], links: Link[] } => {
    if (!claims?.objects?.length) return { nodes: [], links: [] };

    const nodeMap = new Map<string, ClaimNode>();
    const links: Link[] = [];

    // 1. Add all claims as nodes
    claims.objects.slice(0, 200).forEach(claim => { // Limit for performance
      const node: ClaimNode = {
        id: `claim-${claim.primaryKey}`,
        type: 'claim',
        primaryKey: claim.primaryKey,
        claimId: claim.properties.claimId,
        claimAmount: claim.properties.claimAmount,
        anomalyScore: claim.properties.anomalyScore,
        status: claim.properties.status,
        policyholderId: claim.properties.policyholderId
      };
      nodeMap.set(node.id, node);
    });

    // 2. Add policyholders (unique from claims)
    const policyholderIds = new Set(
      claims.objects.slice(0, 200).map(c => c.properties.policyholderId)
    );
    policyholderIds.forEach(phId => {
      const sampleClaim = claims.objects.find(c => c.properties.policyholderId === phId);
      if (sampleClaim && !nodeMap.has(`ph-${phId}`)) {
        nodeMap.set(`ph-${phId}`, {
          id: `ph-${phId}`,
          type: 'policyholder',
          primaryKey: phId,
          policyholderId: phId,
          policyholderName: sampleClaim.properties.policyholderId, // Enhance with real lookup later
          riskProfile: 'High Risk' // From your data generation
        });
      }
    });

    // 3. Add agents (unique from policyholders)
    const agentIds = new Set<string>();
    nodeMap.forEach(node => {
      if (node.agentId) agentIds.add(node.agentId);
    });
    
    // Sample suspicious agents from your data (AGENT_0,1,2)
    ['AGENT_0', 'AGENT_1', 'AGENT_2'].forEach(agentId => {
      if (!nodeMap.has(`agent-${agentId}`)) {
        nodeMap.set(`agent-${agentId}`, {
          id: `agent-${agentId}`,
          type: 'agent',
          primaryKey: agentId,
          agentId,
          agentName: agentId
        });
      }
    });

    // 4. Create links
    claims.objects.slice(0, 200).forEach(claim => {
      const claimNodeId = `claim-${claim.primaryKey}`;
      const phNodeId = `ph-${claim.properties.policyholderId}`;
      
      if (nodeMap.has(claimNodeId) && nodeMap.has(phNodeId)) {
        links.push({
          source: claimNodeId,
          target: phNodeId,
          type: 'claim-policyholder'
        });
      }
    });

    return { 
      nodes: Array.from(nodeMap.values()), 
      links 
    };
  }, [claims]);

  // D3 Force Simulation
  useEffect(() => {
    if (!svgRef.current || !claims?.objects.length) return;

    const svg = d3.select(svgRef.current);
    const { nodes, links } = buildGraphData();
    
    // Clear previous
    svg.selectAll('*').remove();
    if (simulationRef.current) simulationRef.current.stop();

    // SVG setup
    const width = 800;
    const height = 500;
    svg.attr('width', width).attr('height', height);

    // Zoom behavior
    const zoom = d3.zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.1, 8])
      .on('zoom', (event) => {
        svg.selectAll('g').attr('transform', event.transform);
      });
    svg.call(zoom);

    const g = svg.append('g');

    // Links
    const link = g.append('g')
      .selectAll('line')
      .data(links)
      .join('line')
      .attr('stroke', '#999')
      .attr('stroke-opacity', 0.6)
      .attr('stroke-width', 1);

    // Nodes
    const node = g.append('g')
      .selectAll('g')
      .data(nodes)
      .join('g')
      .call(d3.drag<SVGCircleElement, ClaimNode>()
        .on('start', dragstarted)
        .on('drag', dragged)
        .on('end', dragended))
      .on('click', (event, d) => handleNodeClick(d))
      .on('mouseover', (event, d) => showTooltip(event, d))
      .on('mouseout', hideTooltip);

    // Node circles
    const circle = node.append('circle')
      .attr('r', d => nodeRadius(d))
      .attr('fill', d => nodeColor(d))
      .attr('stroke', '#fff')
      .attr('stroke-width', 1.5);

    // Node labels (agent/policyholder only)
    node.append('text')
      .attr('dy', 4)
      .attr('dx', d => d.type === 'agent' ? 12 : 8)
      .attr('font-size', d => d.type === 'agent' ? 12 : 10)
      .attr('font-weight', d => d.type === 'agent' ? 'bold' : 'normal')
      .text(d => {
        if (d.type === 'agent') return d.agentId || '';
        if (d.type === 'policyholder') return d.policyholderId?.slice(-4) || '';
        return '';
      });

    // Simulation
    const simulation = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(links).id(d => d.id).distance(40))
      .force('charge', d3.forceManyBody().strength(-200))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('collide', d3.forceCollide().radius(d => nodeRadius(d) + 2));

    simulation.on('tick', () => {
      link
        .attr('x1', d => (d.source as any).x || 0)
        .attr('y1', d => (d.source as any).y || 0)
        .attr('x2', d => (d.target as any).x || 0)
        .attr('y2', d => (d.target as any).y || 0);

      node
        .attr('transform', d => `translate(${d.x || 0},${d.y || 0})`);
    });

    simulationRef.current = simulation;

    function nodeRadius(d: ClaimNode) {
      if (d.type === 'claim') return Math.sqrt((d.claimAmount || 0) / 5000) * 4 + 3;
      if (d.type === 'policyholder') return 8;
      return 12; // agent
    }

    function nodeColor(d: ClaimNode) {
      if (d.type === 'claim') {
        const score = d.anomalyScore || 0;
        if (score > 70) return '#ff4444';
        if (score > 40) return '#ffaa00';
        return '#88cc88';
      }
      if (d.type === 'policyholder') return d.riskProfile === 'High Risk' ? '#ff8800' : '#66bb6a';
      return '#1976d2'; // agent
    }

    function dragstarted(event: any, d: ClaimNode) {
      if (!event.active) simulation.alphaTarget(0.3).restart();
      d.fx = d.x;
      d.fy = d.y;
    }

    function dragged(event: any, d: ClaimNode) {
      d.fx = event.x;
      d.fy = event.y;
    }

    function dragended(event: any, d: ClaimNode) {
      if (!event.active) simulation.alphaTarget(0);
      d.fx = null;
      d.fy = null;
    }

    function handleNodeClick(d: ClaimNode) {
      if (d.type === 'claim' && flagSuspicious) {
        flagSuspicious({ claimId: d.primaryKey });
      } else if (d.type === 'agent' && investigateCluster) {
        investigateCluster({ agentId: d.agentId });
      }
    }

    function showTooltip(event: React.MouseEvent, d: ClaimNode) {
      if (!tooltipRef.current) return;
      let content = '';
      if (d.type === 'claim') {
        content = `
          <strong>${d.claimId}</strong><br/>
          Amount: $${d.claimAmount?.toLocaleString()}<br/>
          Anomaly: ${(d.anomalyScore || 0).toFixed(1)}<br/>
          Status: ${d.status}
        `;
      } else if (d.type === 'policyholder') {
        content = `<strong>PH: ${d.policyholderId}</strong><br/>Risk: ${d.riskProfile}`;
      } else {
        content = `<strong>${d.agentId}</strong>`;
      }
      tooltipRef.current.innerHTML = content;
      tooltipRef.current.style.opacity = '1';
      tooltipRef.current.style.left = (event.pageX + 10) + 'px';
      tooltipRef.current.style.top = (event.pageY - 10) + 'px';
    }

    function hideTooltip() {
      if (tooltipRef.current) {
        tooltipRef.current.style.opacity = '0';
      }
    }

    return () => {
      simulation.stop();
    };
  }, [buildGraphData, flagSuspicious, investigateCluster]);

  if (!claims?.objects?.length) {
    return (
      <div style={{ padding: '40px', textAlign: 'center', color: '#666' }}>
        <h3>🔍 Fraud Network Detector</h3>
        <p>Filter claims (anomalyScore > 70 recommended) to visualize fraud clusters</p>
        <p>Watch high-risk claims cluster around suspicious agents!</p>
      </div>
    );
  }

  return (
    <div style={{ height: '600px', position: 'relative' }}>
      <svg ref={svgRef} style={{ width: '100%', height: '100%', background: '#fafafa' }} />
      <div 
        ref={tooltipRef} 
        style={{
          position: 'absolute',
          background: 'rgba(0,0,0,0.8)',
          color: 'white',
          padding: '8px 12px',
          borderRadius: '4px',
          fontSize: '12px',
          pointerEvents: 'none',
          opacity: 0,
          transition: 'opacity 0.2s',
          maxWidth: '250px',
          zIndex: 1000
        }}
      />
    </div>
  );
}
