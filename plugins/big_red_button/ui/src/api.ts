const API_BASE = "/big-red-button/api";

export interface FailureInfo {
  dag_id: string;
  task_id: string;
  run_id: string;
  map_index: number;
  state: string;
}

export interface DagFailureSummary {
  dag_id: string;
  failure_count: number;
  failures: FailureInfo[];
}

export interface FailuresResponse {
  total_failures: number;
  dags: DagFailureSummary[];
}

export interface TagInfo {
  name: string;
  selected: boolean;
}

export interface ClearRequest {
  clear_window: string;
  dag_id?: string;
  tags_filter?: string[];
  user?: string;
  user_display?: string;
}

export interface ClearResponse {
  cleared_count: number;
}

export async function fetchFailures(
  clearWindow: string,
  isAdmin: boolean,
  tags?: string[],
  dagId?: string
): Promise<FailuresResponse> {
  const prefix = isAdmin ? `${API_BASE}/admin` : API_BASE;
  const params = new URLSearchParams({ clear_window: clearWindow });
  if (tags && tags.length > 0) {
    tags.forEach((t) => params.append("tags", t));
  }
  if (dagId) {
    params.set("dag_id", dagId);
  }
  const res = await fetch(`${prefix}/failures?${params}`);
  if (!res.ok) throw new Error(`Failed to fetch failures: ${res.statusText}`);
  return res.json();
}

export async function fetchTags(
  selected?: string[]
): Promise<TagInfo[]> {
  const params = new URLSearchParams();
  if (selected && selected.length > 0) {
    selected.forEach((s) => params.append("selected", s));
  }
  const res = await fetch(`${API_BASE}/tags?${params}`);
  if (!res.ok) throw new Error(`Failed to fetch tags: ${res.statusText}`);
  return res.json();
}

export async function clearFailures(
  req: ClearRequest,
  isAdmin: boolean
): Promise<ClearResponse> {
  const prefix = isAdmin ? `${API_BASE}/admin` : API_BASE;
  const res = await fetch(`${prefix}/clear`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(req),
  });
  if (!res.ok) throw new Error(`Failed to clear failures: ${res.statusText}`);
  return res.json();
}
