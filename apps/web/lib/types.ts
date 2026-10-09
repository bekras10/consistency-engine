export type Envelope<T> =
  | { ok: true; data: T }
  | { ok: false; error: string; detail?: string };

export type Level = { price: string; quantity: string };

export type BookView = {
  market_id: string;
  sync_status: string;
  yes_bids: Level[];
  no_bids: Level[];
  yes_asks: Level[];
  no_asks: Level[];
  best_yes_bid: string | null;
  best_yes_ask: string | null;
  best_no_bid: string | null;
  best_no_ask: string | null;
  spread: string | null;
  yes_depth: string;
  no_depth: string;
  received_ts_ms: number;
  source_sequence: number | null;
  depth_note: string;
};

export type DetectionRow = {
  detection_id: string;
  session_id: string;
  relationship_id: string;
  relationship_type: string | null;
  template?: string;
  time_ms: number;
  first_observed_ms: number;
  last_observed_ms: number;
  status: string;
  classification: string;
  duration_ms: number;
  theoretical_deviation: string | null;
  gross_edge: string | null;
  fees: string | null;
  net_edge: string | null;
  max_net_edge: string | null;
  max_deviation: string | null;
  depth_supported_quantity: string | null;
  max_capacity: string | null;
};

export type Overview = {
  data_source: {
    kind: string | null;
    name: string | null;
    label: string | null;
    session_id: string | null;
    session_status: string | null;
    deterministic: boolean | null;
    synthetic: boolean;
  };
  source_health: {
    component: string;
    status: string;
    checked_at: string;
    detail: unknown;
  } | null;
  markets_monitored: number;
  relationships_total: number;
  relationships_verified: number;
  detections_total: number;
  detections_open: number;
  sync_health: Record<string, unknown> | null;
  internal_latency_ns: { p50: number | null; p95: number | null; samples: number; stored: boolean };
  recent_detections: DetectionRow[];
  activity: { start_ms: number; count: number }[];
  empty: boolean;
  seed_hint: string;
};

export type RelationshipListItem = {
  relationship_id: string;
  relationship_type: string;
  verification_status: string;
  categories: string[];
  members: string[];
  member_count: number;
  constraints: string[];
  updated_at: string;
  current_evaluation: { classification: string; status: string } | null;
  proof_detection_id: string | null;
};

export type RelationshipList = {
  relationships: RelationshipListItem[];
  types: string[];
  statuses: string[];
  categories: string[];
};

export type DetectionList = {
  detections: DetectionRow[];
  total: number;
  truncated: boolean;
  classifications: string[];
  relationship_types: string[];
};

export type MarketListItem = {
  market_id: string;
  ticker: string;
  title: string;
  status: string;
  category: string;
  data_source: string;
  event_id: string;
  book?: BookView | null;
};

export type ReplaySnapshot = {
  replay_id: string;
  status: string;
  speed: string;
  cursor: number;
  entry_count: number;
  position_ms: number | null;
  first_ms: number | null;
  last_ms: number | null;
  books: BookView[];
  detections: {
    detection_id: string;
    relationship_id: string;
    status: string;
    classification: string;
    net_edge: string | null;
    max_net_edge: string | null;
    theoretical_deviation: string | null;
  }[];
  events: {
    detection_id: string;
    kind: string;
    at_ms: number;
    position: number;
    classification: string | null;
  }[];
  timeline: { ordinal: number; now_ms: number; kind: string }[];
  sync: Record<string, unknown>;
  transport: string;
};
