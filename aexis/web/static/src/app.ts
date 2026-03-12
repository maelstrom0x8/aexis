import { NetworkVisualizer, PodPositionUpdate } from './visualizer.js';


interface DOMElements {
  activePods: HTMLElement | null;
  operationalStations: HTMLElement | null;
  pendingPassengers: HTMLElement | null;
  systemEfficiency: HTMLElement | null;
  eventStream: HTMLElement | null;
  connectionStatus: HTMLElement | null;
  statusText: HTMLElement | null;
  zoomControl: HTMLInputElement | null;
  zoomLevel: HTMLElement | null;
  offlineOverlay?: HTMLElement | null;
}

interface SystemMetrics {
  active_pods?: number;
  operational_stations?: number;
  pending_passengers?: number;
  system_efficiency?: number;
}

interface UpdateMetricsPayload {
  metrics: SystemMetrics;
  stations?: Record<string, unknown>;
  pods?: Record<string, unknown>;
}

interface WebSocketMessage {
  type: string;
  data?: Record<string, unknown>;
  message?: string;
}

type LogEventType = 'info' | 'warning' | 'error' | 'success';
type StatusType = 'online' | 'offline' | 'warning';

let socket: WebSocket | null = null;
let visualizer: NetworkVisualizer | null = null;
let reconnectInterval: number | null = null;
const MAX_EVENTS = 50;
let posSocket: WebSocket | null = null;

const elements: DOMElements = {
  activePods: document.getElementById('active-pods'),
  operationalStations: document.getElementById('operational-stations'),
  pendingPassengers: document.getElementById('pending-passengers'),
  systemEfficiency: document.getElementById('system-efficiency'),
  eventStream: document.getElementById('event-stream'),
  connectionStatus: document.getElementById('connection-status'),
  statusText: document.getElementById('status-text'),
  zoomControl: document.getElementById('zoom-control') as HTMLInputElement,
  zoomLevel: document.getElementById('zoom-level'),
  offlineOverlay: document.getElementById('offline-overlay')
};

function init(): void {
  try {
    visualizer = new NetworkVisualizer('network-canvas');
  } catch (e) {
    console.error("Visualizer setup failed:", e);
  }

  connectWebSocket();
}


// --- WebSocket Connection ---

async function fetchInitialPodPositions(): Promise<void> {
  // Fetch all pod positions on WebSocket connect and initialize visualizer with them
  try {
    const response = await fetch('/api/pods');
    if (!response.ok) {
      console.error('Failed to fetch pod positions:', response.statusText);
      return;
    }

    const podsData = await response.json();

    if (!visualizer) {
      console.error('Visualizer not initialized');
      return;
    }

    // Initialize each pod in the visualizer with current position and type
    for (const [podId, podState] of Object.entries(podsData)) {
      const pod = podState as Record<string, unknown>;
      // Extract pod type - prefer direct pod_type field
      const podType = (pod.pod_type as string)?.toLowerCase() === 'cargo'
        ? 'cargo'
        : 'passenger';

      // Build position data compatible with handlePodPositionUpdate
      const positionData = {
        pod_id: podId,
        pod_type: podType,
        location: {
          location_type: pod.spine_id ? 'edge' : 'station',
          node_id: !pod.spine_id ? (pod.location as string) : null,
          edge_id: (pod.spine_id as string) || null,
          coordinate: pod.coordinate,
          distance_on_edge: (pod.distance as number) || 0
        },
        status: pod.status || 'idle',
        current_route: pod.current_route || null
      };

      visualizer.handlePodPositionUpdate(positionData as any);
    }
  } catch (error) {
    console.error('Error fetching initial pod positions:', error);
  }
}

function connectWebSocket(): void {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/ws`;
  const posUrl = `${protocol}//${window.location.host}/ws/positions`;

  socket = new WebSocket(wsUrl);
  posSocket = new WebSocket(posUrl);

  socket.onopen = () => {
    updateStatus('Connected', 'online');
    if (reconnectInterval !== null) {
      clearInterval(reconnectInterval);
      reconnectInterval = null;
    }
    if (elements.offlineOverlay) {
      elements.offlineOverlay.classList.add('hidden');
    }
  };

  posSocket.onopen = () => {
    // Fetch initial pod positions and render them
    fetchInitialPodPositions();
  }

  posSocket.onmessage = (event: MessageEvent) => {
    try {
      const data: WebSocketMessage = JSON.parse(event.data);
      handleMessage(data);
    } catch (e) {
      console.error('Failed to parse position message:', e);
    }
  };

  posSocket.onclose = () => {
    
  };

  posSocket.onerror = (error: Event) => {
    
  };

  socket.onmessage = (event: MessageEvent) => {
    try {
      const data: WebSocketMessage = JSON.parse(event.data);
      handleMessage(data);
    } catch (e) {
      console.error('Failed to parse message:', e);
    }
  };

  socket.onclose = () => {
    updateStatus('Disconnected', 'offline');
    if (elements.offlineOverlay) {
      elements.offlineOverlay.classList.remove('hidden');
    }
    if (reconnectInterval === null) {
      reconnectInterval = window.setInterval(connectWebSocket, 3000);
    }
  };

  socket.onerror = (error: Event) => {
    console.error('WebSocket error:', error);
  };
}

// --- Message Handling ---

function handleMessage(payload: WebSocketMessage): void {
  switch (payload.type) {
    case 'system_state':
      updateMetrics(payload.data as unknown as UpdateMetricsPayload);
      break;

    case 'event':
      // Forward real-time events to visualizer
      if (visualizer && payload.data) {
        const channel = (payload as any).channel || '';
        const data: any = payload.data as any;
        visualizer.handleEvent(channel, data);
      }
      break;

    case 'pod_decision':
      if (payload.data) {
        logEvent(
          `Pod ${(payload.data as any).pod_id}: ${(payload.data as any).action}`,
          'info'
        );
      }
      break;

    case 'congestion_alert':
      if (payload.data) {
        logEvent(`Congestion at ${(payload.data as any).station_id}`, 'warning');
      }
      break;

    case 'error':
      logEvent(`System Error: ${payload.message || 'Unknown error'}`, 'error');
      break;
  }
}

// --- UI Updates ---

function updateMetrics(data: UpdateMetricsPayload): void {
  if (!data.metrics) return;

  const { metrics } = data;

  if (elements.activePods) {
    elements.activePods.textContent = String(metrics.active_pods || 0);
  }
  if (elements.operationalStations) {
    elements.operationalStations.textContent = String(metrics.operational_stations || 0);
  }
  if (elements.pendingPassengers) {
    elements.pendingPassengers.textContent = String(metrics.pending_passengers || 0);
  }

  const efficiency = Math.round((metrics.system_efficiency || 0) * 100);
  if (elements.systemEfficiency) {
    elements.systemEfficiency.textContent = `${efficiency}%`;
  }

  if (visualizer) {
    visualizer.updateData(data as unknown as Record<string, unknown>);
  }

}

function updateStatus(text: string, status: StatusType): void {
  const colorMap: Record<StatusType, string> = {
    online: 'text-green-500',
    offline: 'text-red-500',
    warning: 'text-yellow-500'
  };

  if (elements.connectionStatus) {
    const colorClass = colorMap[status] || 'text-gray-400';
    elements.connectionStatus.className = `px-3 py-1 rounded bg-gray-800 text-sm font-bold ${colorClass}`;
    elements.connectionStatus.textContent = text;
  }
}

function logEvent(message: string, type: LogEventType = 'info'): void {
  const div = document.createElement('div');
  const timestamp = new Date().toLocaleTimeString();

  const colorMap: Record<LogEventType, string> = {
    info: 'text-blue-400',
    warning: 'text-yellow-500',
    error: 'text-red-500',
    success: 'text-green-500'
  };

  div.className = `mb-1 font-mono text-sm border-l-2 pl-2 border-gray-700 hover:bg-gray-800 transition-colors py-1`;
  const colorClass = colorMap[type] || 'text-gray-300';
  div.innerHTML = `
        <span class="text-gray-500 text-xs mr-2">[${timestamp}]</span>
        <span class="${colorClass}">${message}</span>
    `;

  if (elements.eventStream) {
    elements.eventStream.insertBefore(div, elements.eventStream.firstChild);

    while (elements.eventStream.children.length > MAX_EVENTS) {
      const last = elements.eventStream.lastChild;
      if (last) {
        elements.eventStream.removeChild(last);
      }
    }
  }
}

// --- Startup ---

document.addEventListener('DOMContentLoaded', init);
