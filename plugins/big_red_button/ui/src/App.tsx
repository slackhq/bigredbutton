import { useState, useEffect, useCallback, useRef } from "react";
import {
  fetchFailures,
  fetchTags,
  clearFailures,
  type FailuresResponse,
  type TagInfo,
  type DagFailureSummary,
} from "./api";
import "./styles.css";

const CLEAR_WINDOWS = [
  { key: "1_hour", label: "1 Hour" },
  { key: "12_hours", label: "12 Hours" },
  { key: "1_day", label: "1 Day" },
  { key: "7_days", label: "7 Days" },
];

function useIsAdmin(): boolean {
  return window.location.pathname.includes("big-red-button-admin");
}

export function App() {
  const isAdmin = useIsAdmin();
  const [clearWindow, setClearWindow] = useState("1_hour");
  const [tags, setTags] = useState<TagInfo[]>([]);
  const [selectedTags, setSelectedTags] = useState<string[]>([]);
  const [failures, setFailures] = useState<FailuresResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [confirmDag, setConfirmDag] = useState<DagFailureSummary | null>(null);
  const [confirmAll, setConfirmAll] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  const loadTags = useCallback(async () => {
    if (isAdmin) return;
    const t = await fetchTags();
    setTags(t);
  }, [isAdmin]);

  const loadFailures = useCallback(async () => {
    if (!isAdmin && selectedTags.length === 0) {
      setFailures(null);
      return;
    }
    setLoading(true);
    try {
      const data = await fetchFailures(
        clearWindow,
        isAdmin,
        !isAdmin && selectedTags.length > 0 ? selectedTags : undefined
      );
      setFailures(data);
    } finally {
      setLoading(false);
    }
  }, [clearWindow, isAdmin, selectedTags]);

  useEffect(() => {
    loadTags();
  }, [loadTags]);

  useEffect(() => {
    loadFailures();
  }, [loadFailures]);

  const handleClearDag = async (dagId: string) => {
    const result = await clearFailures(
      {
        clear_window: clearWindow,
        dag_id: dagId,
        tags_filter: !isAdmin && selectedTags.length > 0 ? selectedTags : undefined,
      },
      isAdmin
    );
    setMessage(`Cleared ${result.cleared_count} task(s) for ${dagId}`);
    setConfirmDag(null);
    loadFailures();
  };

  const handleClearAll = async () => {
    const result = await clearFailures(
      {
        clear_window: clearWindow,
        tags_filter: !isAdmin && selectedTags.length > 0 ? selectedTags : undefined,
      },
      isAdmin
    );
    setMessage(`Cleared ${result.cleared_count} task(s)`);
    setConfirmAll(false);
    loadFailures();
  };

  const addTag = (tagName: string) => {
    if (!selectedTags.includes(tagName)) {
      setSelectedTags((prev) => [...prev, tagName]);
    }
  };

  const removeTag = (tagName: string) => {
    setSelectedTags((prev) => prev.filter((t) => t !== tagName));
  };

  const canClearAll = failures != null && failures.total_failures > 0
    && (isAdmin || selectedTags.length > 0);

  return (
    <div className="brb-container">
      <h1 className="brb-title">
        Big Red Button{isAdmin ? ": Admin" : ""}
      </h1>

      {message && (
        <div className="brb-message" onClick={() => setMessage(null)}>
          {message}
        </div>
      )}

      <div className="brb-controls">
        <div className="brb-control-group">
          <label>Clear failed DAGs in the last:</label>
          <div className="brb-btn-group">
            {CLEAR_WINDOWS.map((w) => (
              <button
                key={w.key}
                className={`brb-btn ${clearWindow === w.key ? "brb-btn-active" : ""}`}
                onClick={() => setClearWindow(w.key)}
              >
                {w.label}
              </button>
            ))}
          </div>
        </div>

        {!isAdmin && (
          <div className="brb-control-group">
            <label>Filter by tags: (required)</label>
            {selectedTags.length > 0 && (
              <div className="brb-selected-tags">
                {selectedTags.map((tag) => (
                  <span key={tag} className="brb-selected-tag">
                    {tag}
                    <button onClick={() => removeTag(tag)} className="brb-selected-tag-remove">&times;</button>
                  </span>
                ))}
                <button className="brb-tag brb-tag-clear" onClick={() => setSelectedTags([])}>
                  Clear all
                </button>
              </div>
            )}
            <TagInput
              allTags={tags.map((t) => t.name)}
              selectedTags={selectedTags}
              onSelect={addTag}
            />
          </div>
        )}

        <button
          className="brb-btn brb-btn-danger"
          disabled={!canClearAll}
          onClick={() => setConfirmAll(true)}
        >
          Clear All Failed DAGs ({failures?.total_failures ?? 0} tasks)
        </button>
      </div>

      {!isAdmin && selectedTags.length === 0 && (
        <div className="brb-empty-state">
          Please select at least one tag to view failures.
        </div>
      )}

      {loading && <div className="brb-loading">Loading...</div>}

      {!loading && failures && (
        <table className="brb-table">
          <thead>
            <tr>
              <th>DAG</th>
              <th>Failures</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {failures.dags.length === 0 ? (
              <tr>
                <td colSpan={3} className="brb-empty">
                  No failures in the last{" "}
                  {CLEAR_WINDOWS.find((w) => w.key === clearWindow)?.label ?? clearWindow}.
                </td>
              </tr>
            ) : (
              failures.dags.map((dag) => (
                <tr key={dag.dag_id}>
                  <td>
                    <a href={`/dags/${dag.dag_id}/grid`} className="brb-dag-link">
                      {dag.dag_id}
                    </a>
                  </td>
                  <td>{dag.failure_count}</td>
                  <td>
                    <button
                      className="brb-btn brb-btn-danger brb-btn-sm"
                      onClick={() => setConfirmDag(dag)}
                    >
                      Clear
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      )}

      {confirmAll && (
        <ConfirmDialog
          title="Clear All Failed DAGs"
          message={`Are you sure you want to clear ${failures?.total_failures ?? 0} failed task(s)?`}
          onConfirm={handleClearAll}
          onCancel={() => setConfirmAll(false)}
        />
      )}

      {confirmDag && (
        <ConfirmDialog
          title={`Clear ${confirmDag.dag_id}`}
          message={`Are you sure you want to clear ${confirmDag.failure_count} failed task(s)?`}
          details={confirmDag.failures.map(
            (f) => `${f.task_id} (run: ${f.run_id}, map: ${f.map_index})`
          )}
          onConfirm={() => handleClearDag(confirmDag.dag_id)}
          onCancel={() => setConfirmDag(null)}
        />
      )}
    </div>
  );
}

function TagInput({
  allTags,
  selectedTags,
  onSelect,
}: {
  allTags: string[];
  selectedTags: string[];
  onSelect: (tag: string) => void;
}) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const wrapperRef = useRef<HTMLDivElement>(null);

  const filtered = allTags.filter(
    (t) => !selectedTags.includes(t) && t.toLowerCase().includes(query.toLowerCase())
  );

  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  return (
    <div className="brb-tag-input-wrapper" ref={wrapperRef}>
      <input
        type="text"
        className="brb-tag-input"
        placeholder="Type to search tags..."
        value={query}
        onChange={(e) => { setQuery(e.target.value); setOpen(true); }}
        onFocus={() => setOpen(true)}
      />
      {open && filtered.length > 0 && (
        <div className="brb-tag-dropdown">
          {filtered.map((tag) => (
            <button
              key={tag}
              className="brb-tag-option"
              onClick={() => { onSelect(tag); setQuery(""); setOpen(false); }}
            >
              {tag}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

function ConfirmDialog({
  title,
  message,
  details,
  onConfirm,
  onCancel,
}: {
  title: string;
  message: string;
  details?: string[];
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <div className="brb-overlay" onClick={onCancel}>
      <div className="brb-dialog" onClick={(e) => e.stopPropagation()}>
        <h2>{title}</h2>
        <p>{message}</p>
        {details && details.length > 0 && (
          <div className="brb-dialog-details">
            {details.map((d, i) => (
              <div key={i} className="brb-dialog-detail">
                {d}
              </div>
            ))}
          </div>
        )}
        <div className="brb-dialog-actions">
          <button className="brb-btn brb-btn-danger" onClick={onConfirm}>
            Confirm
          </button>
          <button className="brb-btn" onClick={onCancel}>
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}
