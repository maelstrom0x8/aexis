interface Vector2 {
  x: number;
  y: number;
}

/**
 * Node: A topological endpoint. The ONLY place where connectivity exists.
 */
interface Node {
  id: string;
  x: number;
  y: number;
  connectedSpines: string[]; // Edge IDs connected to this node
}

/**
 * PathSegment: Geometric primitive.
 */
interface PathSegment {
  start: Vector2;
  end: Vector2;
  length: number; // Precomputed Euclidean distance
  tangent: Vector2; // Normalized direction vector (end - start)
}

/**
 * PathSpine: The authoritative simulation representation of a connection.
 * Composed of ordered segments forming a continuous polyline.
 */
interface PathSpine {
  id: string;
  startNodeId: string;
  endNodeId: string;
  segments: PathSegment[];
  totalLength: number;
}

/**
 * Result of sampling a spine at a specific distance.
 */
interface SpineSample {
  position: Vector2;
  tangent: Vector2;
}

/**
 * Pod: An actor moving along a spine.
 */
interface Pod {
  id: string;
  gfx: PIXI.Graphics;
  podType: "cargo" | "passenger";
  spineId: string;
  distanceAlongPath: number;
  speed: number;
  data: Record<string, unknown>;
  // For interpolation
  targetDistance: number;
  lastUpdate: number;
  velocity: number; // Current server-reported velocity
}

interface VisualizerConfig {
  bgColor: number;
  gridColor: number;
  spineColor: number;
  gridSize: number;
  layoutScale: number;
  tube: {
    width: number;
    glowWidths: number[];
    glowAlphas: number[];
    glowColor: number;
  };
  palette: number[];
}

interface LayoutSpine {
  id: string;
  points: Vector2[];
}

interface NetworkAdjacency {
  node_id: string;
  weight: number;
}

interface NetworkNode {
  id: string;
  label: string;
  coordinate: Vector2;
  adj: NetworkAdjacency[];
}

interface NetworkData {
  nodes: NetworkNode[];
}

/**
 * Visual indicator for payload at a station
 */
interface StationPayload {
  id: string;
  stationId: string;
  type: "passenger" | "cargo";
  gfx: PIXI.Graphics;
  createdAt: number;
}

interface PodLocation {
  location_type: "edge" | "node" | "station";
  node_id: string;
  edge_id: string;
  coordinate: {
    x: number;
    y: number;
  };
  distance_on_edge: number;
}

interface PodPositionUpdate {
  current_route: null | string; // or Route type
  data: Record<string, unknown>;
  event_id: string;
  event_type: "PodPositionUpdate";
  location: PodLocation;
  pod_id: string;
  source: string;
  status: "en_route" | "idle" | "arrived" | "error";
  speed: number; // Added: server-side physics velocity
  timestamp: string; // ISO string
}

class NetworkVisualizer {
  canvas: HTMLElement | null;
  app: PIXI.Application | null;
  zoom: number;
  pan: Vector2;
  config: VisualizerConfig;
  viewport: PIXI.Container | null;
  gridLayer: PIXI.Graphics | null;
  spineLayer: PIXI.Graphics | null;
  nodeLayer: PIXI.Graphics | null;
  podLayer: PIXI.Container | null;
  spines: Map<string, PathSpine>;
  nodes: Map<string, Node>;
  pods: Map<string, Pod>;
  stationPayloads: Map<string, StationPayload>;
  indicatorLayer: PIXI.Container | null;
  labelLayer: PIXI.Container | null = null;
  hoveredLabel: PIXI.Text | null = null;
  hoveredNodeId: string | null = null;

  stationWaitingCounts: Map<string, { passenger: number; cargo: number }> =
    new Map();
  stationDeliveredCounts: Map<
    string,
    { passenger: number; cargo: number; expiresAtMs: number | null }
  > = new Map();
  stationWaitingText: Map<string, PIXI.Text> = new Map();
  stationDeliveredText: Map<string, PIXI.Text> = new Map();

  constructor(canvasId: string) {
    this.canvas = document.getElementById(canvasId);
    this.app = null;

    // Viewport State
    this.zoom = 1.0;
    this.pan = { x: 0, y: 0 };

    // Config
    this.config = {
      bgColor: 0x00050a,
      gridColor: 0x005577,
      spineColor: 0x00fbff,
      gridSize: 100,

      layoutScale: 1,
      tube: {
        width: 1.5,
        glowWidths: [40, 20, 10, 5],
        glowAlphas: [0.05, 0.1, 0.2, 0.4],
        glowColor: 0x00fbff,
      },
      palette: [
        0x00fbff, // Cyan
        0xff8800, // Orange
        0xffff00, // Yellow
        0xff0000, // Red
        0xff00ff, // Purple
      ],
    };

    // State
    this.spines = new Map();
    this.nodes = new Map();
    this.pods = new Map();
    this.stationPayloads = new Map();

    // Layers
    this.viewport = null;
    this.gridLayer = null;
    this.spineLayer = null;
    this.nodeLayer = null;
    this.podLayer = null;
    this.labelLayer = null;
    this.indicatorLayer = null;

    this.init();
  }

  async init(): Promise<void> {
    this.app = new PIXI.Application({
      view: this.canvas as HTMLCanvasElement,
      resizeTo: window,
      antialias: true,
      backgroundColor: this.config.bgColor,
      resolution: window.devicePixelRatio || 1,
    });

    // Setup Containers
    this.viewport = new PIXI.Container();
    this.app.stage.addChild(this.viewport);

    this.gridLayer = new PIXI.Graphics();
    this.viewport.addChild(this.gridLayer);

    this.spineLayer = new PIXI.Graphics();
    this.viewport.addChild(this.spineLayer);

    this.nodeLayer = new PIXI.Graphics();
    this.viewport.addChild(this.nodeLayer);

    this.podLayer = new PIXI.Container();
    this.viewport.addChild(this.podLayer);

    this.indicatorLayer = new PIXI.Container();
    this.viewport.addChild(this.indicatorLayer);

    this.labelLayer = new PIXI.Container();
    this.viewport.addChild(this.labelLayer);

    // Interaction
    this.setupInteraction();

    // Data Load
    await this.loadLayout();

    // Initial draw
    this.drawGrid();
    this.drawSpines();
    this.drawNodes();

    this.labelLayer.interactive = true;
    this.labelLayer.interactiveChildren = false;
    this.labelLayer.hitArea = new PIXI.Rectangle(0, 0, 10000, 10000); // Full canvas

    this.labelLayer.on("pointermove", (event: PIXI.FederatedPointerEvent) => {
      this.handlePodHover(event);
    });

    // Tick
    this.app.ticker.add((delta: number) => this.animate(delta));

    // Handle Resize
    window.addEventListener("resize", () => {
      if (this.app) this.app.resize();
      this.drawGrid();
      this.drawSpines();
    });
  }

  setupInteraction(): void {
    if (!this.app) return;
    this.app.stage.eventMode = "static";
    this.app.stage.hitArea = this.app.screen;

    this.app.stage.on("wheel", (e: WheelEvent) => {
      e.preventDefault();
      const zoomFactor = 1.1;
      const direction = e.deltaY > 0 ? 1 / zoomFactor : zoomFactor;
      const mouseX = (e as any).global?.x ?? e.clientX;
      const mouseY = (e as any).global?.y ?? e.clientY;
      const worldPos = this.toWorld(mouseX, mouseY);

      this.zoom *= direction;
      this.zoom = Math.max(0.1, Math.min(this.zoom, 5.0));
      this.pan.x = mouseX - worldPos.x * this.zoom;
      this.pan.y = mouseY - worldPos.y * this.zoom;
      this.updateViewport();
    });

    let isPanDragging = false;
    let lastMouse = { x: 0, y: 0 };

    this.app.stage.on("pointerdown", (e: PIXI.FederatedPointerEvent) => {
      isPanDragging = true;
      lastMouse = { x: e.global.x, y: e.global.y };
    });
    this.app.stage.on("pointerup", () => (isPanDragging = false));
    this.app.stage.on("pointermove", (e: PIXI.FederatedPointerEvent) => {
      if (isPanDragging) {
        this.pan.x += e.global.x - lastMouse.x;
        this.pan.y += e.global.y - lastMouse.y;
        lastMouse = { x: e.global.x, y: e.global.y };
        this.updateViewport();
      }
    });
  }

  centerView(): void {
    if (!this.app) return;
    this.pan.x = this.app.screen.width / 2;
    this.pan.y = this.app.screen.height / 2;
    this.updateViewport();
  }

  updateViewport(): void {
    if (!this.viewport) return;
    this.viewport.scale.set(this.zoom);
    this.viewport.position.set(this.pan.x, this.pan.y);
    this.drawGrid();
    this.drawSpines();
  }

  toWorld(screenX: number, screenY: number): Vector2 {
    return {
      x: (screenX - this.pan.x) / this.zoom,
      y: (screenY - this.pan.y) / this.zoom,
    };
  }

  private formatStationId(id: string | number): string {
    const numericId = typeof id === "string" ? parseInt(id) : id;
    if (!isNaN(numericId)) {
      return `station_${numericId.toString().padStart(3, "0")}`;
    }
    const idStr = id.toString();
    return idStr.startsWith("station_") ? idStr : `station_${idStr}`;
  }

  drawGrid(): void {
    if (!this.gridLayer || !this.app) return;

    this.gridLayer.clear();
    const gs = this.config.gridSize;
    const startX = -this.pan.x / this.zoom;
    const startY = -this.pan.y / this.zoom;
    const endX = startX + this.app.screen.width / this.zoom;
    const endY = startY + this.app.screen.height / this.zoom;

    // Background Depth
    this.gridLayer.beginFill(0x001122, 0.05);
    this.gridLayer.drawRect(startX, startY, endX - startX, endY - startY);
    this.gridLayer.endFill();

    // Major Grid
    this.gridLayer.lineStyle(1, this.config.gridColor, 0.2);
    for (let x = Math.floor(startX / gs) * gs; x <= endX; x += gs) {
      this.gridLayer.moveTo(x, startY);
      this.gridLayer.lineTo(x, endY);
    }
    for (let y = Math.floor(startY / gs) * gs; y <= endY; y += gs) {
      this.gridLayer.moveTo(startX, y);
      this.gridLayer.lineTo(endX, y);
    }

    // Axis
    this.gridLayer.lineStyle(1, 0x00fbff, 0.1);
    this.gridLayer.moveTo(startX, 0);
    this.gridLayer.lineTo(endX, 0);
    this.gridLayer.moveTo(0, startY);
    this.gridLayer.lineTo(0, endY);
  }

  // Stubs for future implementation
  updateData(data: Record<string, unknown>): void {
    if ((data as any).pods) {
      this.syncPods((data as any).pods);
    }
  }

  /**
   * Handle real-time pod position updates from WebSocket
   */
  handlePodPositionUpdate(positionData: any): void {
    if (!positionData?.pod_id) {
      return;
    }
    console.log("visualizer.handlePodPositionUpdate: received data:", positionData);
    const podId = positionData.pod_id;
    const location = positionData.location;

    // Extract pod type - check positionData first, then fallback to existing pod data
    const podType = (positionData.pod_type ||
      (positionData as any).data?.pod_type ||
      this.pods.get(podId)?.podType ||
      "passenger") as "cargo" | "passenger";

    // console.log(`visualizer.handlePodPositionUpdate: pod ${podId} at location `, location)
    let pod = this.pods.get(podId);
    // console.log("visualizer.handlePodPositionUpdate: found pod with id from  ", this.pods);

    if (!pod) {
      // Create new pod if it doesn't exist
      pod = this.createPod(podId, {
        pod_type: podType,
        spine_id: location?.edge_id || "",
        distance: location?.distance_on_edge || 0,
      });
      this.pods.set(podId, pod);
    }

    // Update pod type and color if changed
    if (pod.podType !== podType) {
      pod.podType = podType;
      this.updatePodColor(pod);
    }

    // Update position data
    if (location) {
      // Correctly track spineId and station status
      const isAtStation =
        location.location_type === "station" || !!location.node_id;

      pod.data = {
        ...pod.data,
        location: location,
        pod_type: podType,
      };

      if (isAtStation) {
        pod.spineId = ""; // No spine if at station
        pod.targetDistance = 0;
        pod.distanceAlongPath = 0;
        // Snap immediately to station coordinate
        if (location.coordinate) {
          pod.gfx.position.set(location.coordinate.x, location.coordinate.y);
        }
      } else {
        // Update spine ID if on edge
        if (location.edge_id && pod.spineId !== location.edge_id) {
          pod.spineId = location.edge_id;
        }

        // Update distance along path
        if (typeof location.distance_on_edge === "number") {
          pod.targetDistance = location.distance_on_edge;
          pod.velocity = positionData.speed || 0;
          pod.lastUpdate = Date.now();

          // Snap if jump is too large (e.g. initial load), snap
          const distDiff = Math.abs(pod.targetDistance - pod.distanceAlongPath);
          if (distDiff > 100 || pod.spineId !== location.edge_id) {
            pod.distanceAlongPath = pod.targetDistance;
          }
        }
      }
    }
  }

  updatePodColor(pod: Pod): void {
    // Update pod graphics color based on pod type (cargo=orange, passenger=teal)
    const podColor = pod.podType === "cargo" ? 0xff8800 : 0x00fbff; // Orange for cargo, teal for passenger

    // Redraw pod with new color
    pod.gfx.clear();

    // Core pod circle
    pod.gfx.beginFill(podColor, 1);
    pod.gfx.drawCircle(0, 0, 3);
    pod.gfx.endFill();

    // Glow aura based on pod type
    pod.gfx.beginFill(podColor, 0.3);
    pod.gfx.drawCircle(0, 0, 6);
    pod.gfx.endFill();
  }

  /**
   * Handle real-time events from WebSocket
   */
  handleEvent(channel: string, eventData: any): void {
    const eventType = eventData.event_type;

    if (eventType.includes("PodPositionUpdate")) {
      this.handlePodPositionUpdate(eventData as PodPositionUpdate);
    }

    // Waiting payloads at station (count-based)
    if (eventType.includes("PassengerArrival")) {
      this.incrementWaiting(eventData.station_id, "passenger", 1);
    } else if (eventType.includes("CargoRequest")) {
      this.incrementWaiting(eventData.origin, "cargo", 1);
    }

    // Pickup/removal from origin station (count-based)
    if (eventType.includes("PassengerPickedUp")) {
      this.incrementWaiting(eventData.station_id, "passenger", -1);
    } else if (eventType.includes("CargoLoaded")) {
      this.incrementWaiting(eventData.station_id, "cargo", -1);
    }

    // Delivery flash at destination station (count-based, expires)
    if (eventType.includes("PassengerDelivered")) {
      this.incrementDelivered(eventData.station_id, "passenger", 1);
    } else if (eventType.includes("CargoDelivered")) {
      this.incrementDelivered(eventData.station_id, "cargo", 1);
    }
  }

  private incrementWaiting(
    stationId: string | undefined,
    type: "passenger" | "cargo",
    delta: number,
  ): void {
    if (!stationId) return;
    const formattedId = this.formatStationId(stationId);
    const current = this.stationWaitingCounts.get(formattedId) || {
      passenger: 0,
      cargo: 0,
    };
    current[type] = Math.max(0, (current[type] || 0) + delta);
    this.stationWaitingCounts.set(formattedId, current);
    // console.log(
    //   "visualizer.incrementWaiting: station waiting counts:",
    //   this.stationWaitingCounts,
    // );
    this.renderWaitingIndicator(formattedId);
  }

  private incrementDelivered(
    stationId: string | undefined,
    type: "passenger" | "cargo",
    delta: number,
  ): void {
    if (!stationId) return;
    const now = Date.now();
    const formattedId = this.formatStationId(stationId);
    const current = this.stationDeliveredCounts.get(formattedId) || {
      passenger: 0,
      cargo: 0,
      expiresAtMs: null as number | null,
    };
    current[type] = Math.max(0, (current[type] || 0) + delta);
    current.expiresAtMs = now + 2000;
    this.stationDeliveredCounts.set(formattedId, current);
    this.renderDeliveredIndicator(formattedId);
  }

  private renderWaitingIndicator(stationId: string): void {
    if (!this.indicatorLayer) return;
    const formattedId = this.formatStationId(stationId);
    const counts = this.stationWaitingCounts.get(formattedId);
    const total = (counts?.passenger || 0) + (counts?.cargo || 0);
    const node = this.nodes.get(formattedId);
    if (!node) return;

    const existing = this.stationWaitingText.get(formattedId);
    if (total <= 0) {
      if (existing) {
        this.indicatorLayer.removeChild(existing);
        existing.destroy();
        this.stationWaitingText.delete(stationId);
      }
      return;
    }

    const passengerCount = counts?.passenger || 0;
    const cargoCount = counts?.cargo || 0;
    const label = `P:${passengerCount} C:${cargoCount}`;

    const style = new PIXI.TextStyle({
      fontFamily: "monospace",
      fontSize: 12,
      fill: 0xffffff,
      stroke: 0x000000,
      strokeThickness: 2,
    });

    const txt = existing || new PIXI.Text(label, style);
    txt.text = label;
    txt.x = node.x + 10;
    txt.y = node.y + 10;
    if (!existing) {
      this.stationWaitingText.set(formattedId, txt);
      this.indicatorLayer.addChild(txt);
    }
  }

  private renderDeliveredIndicator(stationId: string): void {
    if (!this.indicatorLayer) return;
    const formattedId = this.formatStationId(stationId);
    const counts = this.stationDeliveredCounts.get(formattedId);
    const total = (counts?.passenger || 0) + (counts?.cargo || 0);
    const node = this.nodes.get(formattedId);
    if (!node) return;

    const existing = this.stationDeliveredText.get(formattedId);
    if (total <= 0) {
      if (existing) {
        this.indicatorLayer.removeChild(existing);
        existing.destroy();
        this.stationDeliveredText.delete(stationId);
      }
      return;
    }

    const passengerCount = counts?.passenger || 0;
    const cargoCount = counts?.cargo || 0;
    const label = `DEL P:${passengerCount} C:${cargoCount}`;

    const style = new PIXI.TextStyle({
      fontFamily: "monospace",
      fontSize: 12,
      fill: 0x00ff66,
      stroke: 0x000000,
      strokeThickness: 2,
    });

    const txt = existing || new PIXI.Text(label, style);
    txt.text = label;
    txt.x = node.x + 10;
    txt.y = node.y - 24;
    txt.alpha = 1.0;
    if (!existing) {
      this.stationDeliveredText.set(formattedId, txt);
      this.indicatorLayer.addChild(txt);
    }
  }

  syncPods(podsData: Record<string, any>): void {
    const now = Date.now();

    // 1. Mark all existing pods as unseen
    const unseenIds = new Set(this.pods.keys());

    for (const [id, data] of Object.entries(podsData)) {
      let pod = this.pods.get(id);
      unseenIds.delete(id);

      if (!pod) {
        pod = this.createPod(id, data);
        this.pods.set(id, pod);
      }

      // Update authoritative state
      pod.data = data;

      // If spine changed, jump to it (or handle transition if advanced)
      if (data.spine_id && pod.spineId !== data.spine_id) {
        pod.spineId = data.spine_id;
        // When switching spines, we might want to snap or interpolate.
        // For now, snap to ensure correctness.
        pod.distanceAlongPath = data.distance ?? 0;
      }

      // Update target for interpolation
      // Assuming backend sends 'distance' property
      if (typeof data.distance === "number") {
        // Simple interpolation setup:
        // We know where it is NOW (visual), and where server says it is (target).
        pod.targetDistance = data.distance;
        pod.lastUpdate = now;

        // If the jump is too large (e.g. initial load), snap
        if (Math.abs(pod.targetDistance - pod.distanceAlongPath) > 100) {
          pod.distanceAlongPath = pod.targetDistance;
        }
      }
    }

    // 2. Remove stale pods
    unseenIds.forEach((id) => {
      const pod = this.pods.get(id);
      if (pod) {
        pod.gfx.destroy(); // Remove from scene
        this.pods.delete(id);
      }
    });
  }

  createPod(id: string, data: any): Pod {
    const podType: "cargo" | "passenger" =
      data.pod_type === "cargo" ? "cargo" : "passenger";
    const podColor = podType === "cargo" ? 0xff8800 : 0x00fbff; // Orange for cargo, teal for passenger

    const gfx = new PIXI.Graphics();

    // Core pod circle with color based on type
    gfx.beginFill(podColor, 1);
    gfx.drawCircle(0, 0, 3);
    gfx.endFill();

    // Glow aura
    gfx.beginFill(podColor, 0.3);
    gfx.drawCircle(0, 0, 6);
    gfx.endFill();

    if (this.podLayer) this.podLayer.addChild(gfx);

    return {
      id,
      gfx,
      podType,
      spineId: data.spine_id || "",
      distanceAlongPath: data.distance || 0,
      targetDistance: data.distance || 0,
      speed: 0, // Speed is derived from server updates now
      velocity: 0, // Current movement velocity for extrapolation
      lastUpdate: Date.now(),
      data: data,
    };
  }

  animate(delta: number): void {
    // Current time delta in seconds (assuming 60fps baseline for PIXI delta=1.0)
    const dt = delta / 60;
    const lerpFactor = 0.15; // Increased slightly for snappier catch-up

    this.pods.forEach((pod) => {
      // If the pod is at a station (no spineId), we don't interpolate distance,
      // its position was already set in handlePodPositionUpdate.
      if (!pod.spineId) return;

      const spine = this.spines.get(pod.spineId);
      if (!spine) return;

      // 1. Extrapolation: Predict movement based on last known velocity
      if (pod.velocity > 0) {
        pod.distanceAlongPath += pod.velocity * dt;
      }

      // 2. Correction: Gently pull towards the authoritative server position (targetDistance)
      // This prevents divergence over time while maintaining smooth motion
      const error = pod.targetDistance - pod.distanceAlongPath;

      // If error is small, use lerp to smooth it out. If massive, we already snapped in the handler.
      if (Math.abs(error) > 0.1) {
        pod.distanceAlongPath += error * lerpFactor;
      }

      // Bind to spine length
      pod.distanceAlongPath = Math.max(
        0,
        Math.min(pod.distanceAlongPath, spine.totalLength),
      );

      // authoritative sample & render
      const sample = this.sampleSpine(spine, pod.distanceAlongPath);
      pod.gfx.position.set(sample.position.x, sample.position.y);

      // Orient towards tangent
      pod.gfx.rotation = Math.atan2(sample.tangent.y, sample.tangent.x);
    });

    // Delivered indicator expiry & fade
    const now = Date.now();
    for (const [
      stationId,
      delivered,
    ] of this.stationDeliveredCounts.entries()) {
      if (!delivered.expiresAtMs) continue;
      const remaining = delivered.expiresAtMs - now;
      const txt = this.stationDeliveredText.get(stationId);

      if (remaining <= 0) {
        this.stationDeliveredCounts.set(stationId, {
          passenger: 0,
          cargo: 0,
          expiresAtMs: null,
        });
        if (txt && this.indicatorLayer) {
          this.indicatorLayer.removeChild(txt);
          txt.destroy();
          this.stationDeliveredText.delete(stationId);
        }
        continue;
      }

      if (txt) {
        txt.alpha = Math.max(0.0, Math.min(1.0, remaining / 2000));
      }
    }
  }

  /**
   * Offline Routing: REMOVED
   * The visualizer is now a dumb terminal. Routing happens on the backend.
   */

  async loadLayout(): Promise<void> {
    try {
      const response = await fetch("/api/network");
      const data = (await response.json()) as NetworkData;
      // Apply layout scale to coordinates if needed, or assume they are pre-scaled
      // For this network.json, coordinates look like -700, 800, so likely no scale needed or scale = 1
      if (this.config.layoutScale !== 1) {
        data.nodes.forEach((n) => {
          n.coordinate.x *= this.config.layoutScale;
          n.coordinate.y *= this.config.layoutScale;
        });
      }

      this.generateLayout(data);
    } catch (e) {
      console.error("Failed to load network.json", e);
    }
  }

  generateLayout(data: NetworkData): void {
    this.spines.clear();
    this.nodes.clear();

    if (!data.nodes) return;

    // 1. Create Nodes
    data.nodes.forEach((n) => {
      const paddedId = this.formatStationId(n.id);
      this.nodes.set(paddedId, {
        id: paddedId,
        x: n.coordinate.x,
        y: n.coordinate.y,
        connectedSpines: [],
      });
    });

    // 2. Create Spines from Adjacency (Edges)
    const seenEdges = new Set<string>();

    data.nodes.forEach((sourceNode) => {
      if (!sourceNode.adj) return;

      const paddedSourceId = this.formatStationId(sourceNode.id);
      const startNode = this.nodes.get(paddedSourceId);
      if (!startNode) return;

      sourceNode.adj.forEach((adj) => {
        const paddedTargetId = this.formatStationId(adj.node_id);
        const endNode = this.nodes.get(paddedTargetId);

        if (!endNode) return;

        // Simple deduplication: "minId-maxId"
        // For routing, we treat this edge as bidirectional conceptually,
        // but our simulation moves along one vector.
        // To allow bidirectional travel, we might need TWO spines (A->B and B->A)
        // OR intelligent traversal that handles negative speed.
        // Simplest for now: Create two directed spines for every link so pods can flow both ways easily.
        // Wait, typically edges are undirected. Let's stick to unique edges and handle "reverse" traversal later.
        // Actually, for "pickNextRoute" to work easily with "distanceAlongPath",
        // it's easiest if spines are directed paths A->B.
        // If the graph is undirected, we should probably generate TWO directed spines per adjacency
        // so we don't have to handle "moving backwards" logic right now.

        // Let's create directed spines for EVERY adjacency.
        // edgeId = "source->target"
        const edgeId = `${paddedSourceId}->${paddedTargetId}`;

        // Generate geometry (Octilinear path between nodes)
        const pathPoints = this.getOctilinearPath(
          startNode.x,
          startNode.y,
          endNode.x,
          endNode.y,
        );

        const spine = this.createSpine(pathPoints, edgeId, startNode, endNode);
        // console.log("Setting spine: ", edgeId, spine);
        this.spines.set(edgeId, spine);

        // Register connection
        startNode.connectedSpines.push(edgeId);
        // We don't push to endNode because this spine flows AWAY from startNode.
        // endNode will have its own outgoing spine created when we iterate over *its* adj list.
      });
    });

    // Inject Mock Pods: REMOVED
    // We now rely on the backend to send pods in updateData()
    this.pods.clear();
    if (this.podLayer) this.podLayer.removeChildren();

    this.centerView();
    this.drawSpines();
    this.drawNodes();
  }

  setPaused(p: boolean): void {}
  resetView(): void {
    this.zoom = 1.0;
    this.pan = {
      x: this.app?.screen.width || 0 / 2,
      y: this.app?.screen.height || 0 / 2,
    };
    this.updateViewport();
  }

  /**
   * Factory to create a PathSpine from an ordered list of points.
   * Precomputes all segment lengths and tangents for O(1) sampling lookup.
   */
  createSpine(
    points: Vector2[],
    id: string,
    startNode: Node,
    endNode: Node,
  ): PathSpine {
    const segments: PathSegment[] = [];
    let totalLength = 0;

    for (let i = 0; i < points.length - 1; i++) {
      const start = points[i];
      const end = points[i + 1];
      const dx = end.x - start.x;
      const dy = end.y - start.y;
      const length = Math.sqrt(dx * dx + dy * dy);

      if (length > 0) {
        segments.push({
          start,
          end,
          length,
          tangent: { x: dx / length, y: dy / length },
        });
        totalLength += length;
      }
    }

    return {
      id,
      startNodeId: startNode.id,
      endNodeId: endNode.id,
      segments,
      totalLength,
    };
  }

  /**
   * Generate path between two points. For now, just a straight line.
   * Can be extended for curved or multi-segment paths later.
   */
  getOctilinearPath(x1: number, y1: number, x2: number, y2: number): Vector2[] {
    // Simplified: just return start and end points for a straight line
    return [
      { x: x1, y: y1 },
      { x: x2, y: y2 },
    ];
  }

  /**
   * Core simulation sampler. Returns world position and tangent at dist scalar.
   */
  sampleSpine(spine: PathSpine, dist: number): SpineSample {
    if (spine.segments.length === 0) {
      return { position: { x: 0, y: 0 }, tangent: { x: 1, y: 0 } };
    }

    // Clamp distance to spine bounds
    const clampedDist = Math.max(0, Math.min(dist, spine.totalLength));
    let accumulatedDist = 0;

    for (const seg of spine.segments) {
      if (accumulatedDist + seg.length >= clampedDist) {
        const localDist = clampedDist - accumulatedDist;
        const t = localDist / seg.length;

        return {
          position: {
            x: seg.start.x + (seg.end.x - seg.start.x) * t,
            y: seg.start.y + (seg.end.y - seg.start.y) * t,
          },
          tangent: seg.tangent,
        };
      }
      accumulatedDist += seg.length;
    }

    // Fallback to end of last segment
    const last = spine.segments[spine.segments.length - 1];
    return { position: last.end, tangent: last.tangent };
  }

  /**
   * Renders high-fidelity TRON "tubes" derived strictly from the authoritative spines.
   */
  drawSpines(): void {
    if (!this.spineLayer) return;
    this.spineLayer.clear();

    let i = 0;
    for (const spine of this.spines.values()) {
      const color = this.config.palette[i % this.config.palette.length];
      this.drawDerivedStrokes(spine, color);
      i++;
    }
  }

  drawDerivedStrokes(spine: PathSpine, color: number): void {
    if (!this.spineLayer) return;

    // We can draw a single centered tube, or multiple parallel ones
    const offsets = [0]; // Just center for now, but we can add [-5, 5] for double lines

    offsets.forEach((offset) => {
      // 1. Layered Glows
      // this.config.tube.glowWidths.forEach((width, idx) => {
      //     this.spineLayer!.lineStyle(width, this.config.tube.glowColor, this.config.tube.glowAlphas[idx]);
      //     this.drawLayeredPath(spine, offset);
      // });

      // // 2. Core Brilliant Line
      // this.spineLayer.lineStyle(this.config.tube.width, 0xffffff, 0.9);
      // this.drawLayeredPath(spine, offset);

      // 3. Primary Color Inner Line
      this.spineLayer.lineStyle(this.config.tube.width - 1, color, 1);
      this.drawLayeredPath(spine, offset);
    });
  }

  drawLayeredPath(spine: PathSpine, offset: number): void {
    if (!this.spineLayer || spine.segments.length === 0) return;

    for (let i = 0; i < spine.segments.length; i++) {
      const seg = spine.segments[i];
      const normal = { x: -seg.tangent.y, y: seg.tangent.x };

      const startX = seg.start.x + normal.x * offset;
      const startY = seg.start.y + normal.y * offset;
      const endX = seg.end.x + normal.x * offset;
      const endY = seg.end.y + normal.y * offset;

      if (i === 0) {
        this.spineLayer.moveTo(startX, startY);
      }
      this.spineLayer.lineTo(endX, endY);
    }
  }

  getPodAtPosition(x: number, y: number, radius: number = 15): any | null {
    for (const pod of this.pods.values()) {
      const coordinate = pod.data?.coordinate as { x: number; y: number };
      const dist = Math.sqrt(
        (x - coordinate?.x) ** 2 + (y - coordinate?.y) ** 2,
      );
      if (dist <= radius) return pod;
    }
    return null;
  }

  showPodLabel(pod: any): void {
    const style = new PIXI.TextStyle({
      fontFamily: "monospace",
      fontSize: 12,
      fill: 0xffffff,
      stroke: { color: 0x000000, width: 1 },
      dropShadow: true,
      dropShadowColor: 0x000000,
      dropShadowBlur: 2,
    });

    const labelText = pod.id;
    this.hoveredLabel = new PIXI.Text(labelText, style);

    // Top-right positioning
    this.hoveredLabel.x = pod.x + 15;
    this.hoveredLabel.y = pod.y - 20;

    this.labelLayer!.addChild(this.hoveredLabel);
  }

  handlePodHover(event: PIXI.FederatedPointerEvent): void {
    const pos = event.getLocalPosition(this.podLayer!);
    const pod = this.getPodAtPosition(pos.x, pos.y);

    if (pod && pod.id !== this.hoveredNodeId) {
      // Hide previous label
      if (this.hoveredLabel) {
        this.labelLayer!.removeChild(this.hoveredLabel);
        this.hoveredLabel.destroy();
        this.hoveredLabel = null;
      }

      // Show new label
      this.showPodLabel(pod);
      this.hoveredNodeId = pod.id;
    } else if (!pod && this.hoveredNodeId) {
      // Hide label when no node hovered
      if (this.hoveredLabel) {
        this.labelLayer!.removeChild(this.hoveredLabel);
        this.hoveredLabel.destroy();
        this.hoveredLabel = null;
      }
      this.hoveredNodeId = null;
    }
  }

  drawPodLabel(): void {}

  drawNodeLabel(): void {
    if (!this.labelLayer) return;
    this.labelLayer.removeChildren(); // Clear previous labels

    const style = new PIXI.TextStyle({
      fontFamily: "Arial",
      fontSize: 12,
      fill: "#ffffff",
      stroke: "#000000",
      strokeThickness: 3,
      dropShadow: {
        color: "#000000",
        distance: 4,
        angle: Math.PI / 4,
        alpha: 0.5,
      },
    });

    for (const node of this.nodes.values()) {
      let labelText = `${node.id}`;
      labelText = labelText.replace("station_", "S");
      labelText = labelText.replace("_", "");
      const label = new PIXI.Text(labelText, style);

      // Position: top-right of node (offset by radius + padding)
      label.x = node.x + 15; // Right of center (12px radius + 3px pad)
      label.y = node.y - 20; // Above center (label height/2 ~15px + pad)

      // Optional: anchor top-left so x/y is exact top-left corner
      // label.anchor.set(0, 0);
      this.labelLayer.addChild(label);
    }
  }
  /**
   * Renders topological nodes.
   */
  drawNodes(): void {
    if (!this.nodeLayer) return;
    this.nodeLayer.clear();

    // Simple glowing dot for nodes
    for (const node of this.nodes.values()) {
      this.nodeLayer.beginFill(0x00fbff, 0.3);
      this.nodeLayer.drawCircle(node.x, node.y, 12);
      this.nodeLayer.endFill();

      this.nodeLayer.beginFill(0xffffff, 1);
      this.nodeLayer.drawCircle(node.x, node.y, 3.5);
      this.nodeLayer.endFill();
    }

    this.drawNodeLabel();
  }
}

export { NetworkVisualizer, PodLocation, PodPositionUpdate };
