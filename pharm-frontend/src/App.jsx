import { useEffect, useMemo, useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer
} from "recharts";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

function shekels(n) {
  if (n == null) return "—";
  return new Intl.NumberFormat("he-IL", { style: "currency", currency: "ILS" }).format(n);
}

export default function App() {
  const [q, setQ] = useState("");
  const [loading, setLoading] = useState(false);
  const [rows, setRows] = useState([]);
  const [error, setError] = useState("");
  const [selected, setSelected] = useState(null); // { barcode, name, ... }
  const [detail, setDetail] = useState(null);     // { product, current_price }
  const [history, setHistory] = useState([]);

  async function doSearch(query) {
    setLoading(true);
    setError("");
    try {
      const url = `${API_BASE}/search?q=${encodeURIComponent(query)}&limit=50&store_id=072`;
      const r = await fetch(url);
      const j = await r.json();
      setRows(j.results || []);
    } catch (e) {
      setError("שגיאת רשת בחיפוש");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { doSearch(""); }, []);

  async function openItem(row) {
    setSelected(row);
    setDetail(null);
    setHistory([]);
    try {
      const d = await fetch(`${API_BASE}/item/${row.barcode}?store_id=072`).then(r => r.json());
      setDetail(d);
      const h = await fetch(`${API_BASE}/item/${row.barcode}/history?days=60&store_id=072`).then(r => r.json());
      const data = (h.history || []).map(x => ({
        t: x.observed_at?.slice(0, 10),
        price: x.price,
        per100: x.unit_price_per100,
      }));
      setHistory(data);
    } catch (e) {
      // ignore; keep modal open
    }
  }

  function Badge({ hasPromo, reward }) {
    if (!hasPromo) return null;
    return (
      <span className="ml-2 inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium bg-amber-100 text-amber-800">
        במבצע{reward ? ` · ${reward}` : ""}
      </span>
    );
  }

  const table = useMemo(() => (
    <div className="overflow-x-auto rounded-2xl border border-gray-200 shadow-sm">
      <table className="min-w-full text-sm">
        <thead className="bg-gray-50">
          <tr className="text-left">
            <th className="p-3">שם</th>
            <th className="p-3">מחיר</th>
            <th className="p-3">₪/100</th>
            <th className="p-3">יח'</th>
            <th className="p-3">ברקוד</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr
              key={r.barcode}
              className="border-t hover:bg-gray-50 cursor-pointer"
              onClick={() => openItem(r)}
              title="פרטים וגרף היסטוריה"
            >
              <td className="p-3">
                <div className="flex items-center justify-between gap-2">
                  <span>{r.name}</span>
                  <Badge hasPromo={!!r.has_promo} reward={r.reward_types} />
                </div>
              </td>
              <td className="p-3">{shekels(r.price)}</td>
              <td className="p-3">{r.unit_price_per100 != null ? shekels(r.unit_price_per100) : "—"}</td>
              <td className="p-3">{r.qty_unit || "—"}</td>
              <td className="p-3 font-mono">{r.barcode}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  ), [rows]);

  return (
    <div className="mx-auto max-w-5xl p-6 space-y-6">
      <header className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold">מחירון סופר-פארם — דמו</h1>
      </header>

      <div className="flex gap-2">
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && doSearch(q)}
          placeholder="חיפוש (למשל: שמפו, חיתולים, אריאל)…"
          className="flex-1 rounded-xl border border-gray-300 px-3 py-2 outline-none focus:ring-2 focus:ring-blue-400"
        />
        <button
          onClick={() => doSearch(q)}
          className="rounded-xl bg-blue-600 text-white px-4 py-2 hover:bg-blue-700"
        >
          חיפוש
        </button>
      </div>

      {loading && <div className="text-gray-500">טוען…</div>}
      {error && <div className="text-red-600">{error}</div>}
      {!loading && table}

      {/* Item drawer */}
      {selected && (
        <div className="fixed inset-0 bg-black/30 flex" onClick={() => setSelected(null)}>
          <div
            className="ml-auto h-full w-full max-w-xl bg-white shadow-xl p-6 overflow-y-auto"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-start justify-between">
              <h2 className="text-lg font-semibold">{selected.name}</h2>
              <button onClick={() => setSelected(null)} className="text-gray-500 hover:text-gray-700">✕</button>
            </div>

            {detail ? (
              <div className="mt-4 space-y-2 text-sm">
                <div><b>ברקוד:</b> <span className="font-mono">{selected.barcode}</span></div>
                <div><b>מחיר נוכחי:</b> {shekels(detail?.current_price?.price)}</div>
                <div><b>₪/100:</b> {detail?.current_price?.unit_price_per100 != null ? shekels(detail.current_price.unit_price_per100) : "—"}</div>
                <div><b>מבצע:</b> {detail?.current_price?.has_promo ? `כן (${detail.current_price.reward_types || ""})` : "לא"}</div>
                <div><b>עודכן:</b> {detail?.current_price?.last_seen_at}</div>
              </div>
            ) : (
              <div className="mt-6 text-gray-500">טוען פרטים…</div>
            )}

            <div className="mt-6">
              <h3 className="font-medium mb-2">היסטוריית מחיר (60 ימים)</h3>
              <div className="h-48 w-full border rounded-xl p-2">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={history}>
                    <XAxis dataKey="t" hide />
                    <YAxis />
                    <Tooltip />
                    <Line type="monotone" dataKey="price" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
