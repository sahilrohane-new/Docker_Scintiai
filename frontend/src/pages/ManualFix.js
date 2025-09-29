import React, { useEffect, useState } from "react";
import axios from "../api/axiosInstance";
import MonacoEditor from "react-monaco-editor";
import "../pages/UploadSAS.css";

const ManualFix = () => {
  const [chunks, setChunks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [results, setResults] = useState({});

  useEffect(() => {
    fetchFailedChunks();
  }, []);

  const fetchFailedChunks = async () => {
    try {
      const res = await axios.get("/manual_review_chunks");
      setChunks(res.data || []);
    } catch (err) {
      console.error("Failed to load manual chunks", err);
    } finally {
      setLoading(false);
    }
  };

  const handleRevalidate = async (chunk) => {
    try {
      const res = await axios.post("/revalidate_chunk", {
        id: chunk.id,
        code: chunk.fixed_code,
      });

      setResults((prev) => ({
        ...prev,
        [chunk.id]: res.data.validated ? "✅ Passed" : `❌ ${res.data.reason}`,
      }));

      if (res.data.validated) {
        // remove from current list
        setChunks((prev) => prev.filter((c) => c.id !== chunk.id));
      }
    } catch (err) {
      setResults((prev) => ({
        ...prev,
        [chunk.id]: "⚠️ Revalidation failed",
      }));
    }
  };

  const handleCodeChange = (id, newCode) => {
    setChunks((prev) =>
      prev.map((chunk) =>
        chunk.id === id ? { ...chunk, fixed_code: newCode } : chunk
      )
    );
  };

  if (loading) return <div className="loading">Loading failed chunks...</div>;

  if (chunks.length === 0) return <div className="success">🎉 No manual fixes needed!</div>;

  return (
    <div className="manual-fix-page">
      <h2>🔧 Manual Fix Required</h2>
      <p>Edit the failed PySpark chunks below and click Revalidate.</p>

      {chunks.map((chunk) => (
        <div key={chunk.id} className="chunk-editor-box">
          <h4>{chunk.id}</h4>
          <p className="reason">❌ {chunk.reason}</p>
          <MonacoEditor
            height="200"
            language="python"
            value={chunk.fixed_code || chunk.pyspark_code}
            onChange={(value) => handleCodeChange(chunk.id, value)}
            theme="vs-dark"
            options={{ minimap: { enabled: false } }}
          />
          <button onClick={() => handleRevalidate(chunk)}>Revalidate</button>
          {results[chunk.id] && <p className="result">{results[chunk.id]}</p>}
        </div>
      ))}
    </div>
  );
};

export default ManualFix;