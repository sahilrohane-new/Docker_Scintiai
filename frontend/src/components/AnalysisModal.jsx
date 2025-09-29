import React from "react";
import { Dialog, DialogTitle, DialogContent, IconButton, Stack, Typography, Button } from "@mui/material";
import { toast } from "react-toastify"; 
import CloseIcon from "@mui/icons-material/Close";
import { PieChart, Pie, Cell, Tooltip, BarChart, XAxis, YAxis, Bar, ResponsiveContainer } from "recharts";
import axiosInstance from "../api/axiosInstance";
import fileDownload from "js-file-download";

// const isPythonTarget = (path) => {
//   if (!path) return true;
//   return /_(pyspark|snowpark)\./i.test(path);  // treat these as Python
// };

// const getFinalFallbackName = (path) =>
//   isPythonTarget(path) ? "final_optimized_code.py" : "final_optimized_code.sql";

// const getBeforeFallbackName = (path) =>
//   isPythonTarget(path) ? "before_optimization.py" : "before_optimization.sql";


const COLORS = ["#8884d8", "#82ca9d", "#ffc658", "#ff8042"];
// helper
const normalizeUrl = (u) =>
    /^https?:\/\//i.test(u) ? u : axiosInstance.defaults.baseURL + u;
const doDownload = async (url, fallbackName) => {
  try {
    // const res = await axiosInstance.get(url, { responseType: "blob" });
    const res = await axiosInstance.get(normalizeUrl(url), { responseType: "blob" });
    const cd  = res.headers["content-disposition"] || "";
    const fnameMatch = cd.match(/filename="?([^"]+)"?/);
    const filename = fnameMatch ? fnameMatch[1] : fallbackName;
    fileDownload(res.data, filename);
  } catch {
    toast.error("Download failed");
  }
};


export default function AnalysisModal({ open, onClose, report, downloadPath, jobID }) {
  if (!report) return null;

  const target = report.summary?.target?.toLowerCase() || "pyspark";

  const EXT = { pyspark: ".py", snowpark: ".py", python: ".py", matillion: ".json", dbt: ".yml" };
  const ext = EXT[target] || ".sql";
  const base = report.summary?.input_basename;
  const finalName  = `Optimized_${base}${ext}`;
  const beforeName = `Before_Optimized_${base}${ext}`;

  // const isPython = ["pyspark", "snowpark"].includes(target);

  const stageData = [
    { name: "LLM In",  value: report.llm_usage.by_stage.llm.input  },
    { name: "LLM Out", value: report.llm_usage.by_stage.llm.output },
    { name: "Opt In",  value: report.llm_usage.by_stage.optimize.input },
    { name: "Opt Out", value: report.llm_usage.by_stage.optimize.output },
  ];
  const optData = [
    { name: "Before", lines: report.optimization.lines_before },
    { name: "After",  lines: report.optimization.lines_after  },
  ];

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle sx={{pr:5}}>
        Conversion Analysis
        <IconButton onClick={onClose} sx={{ position:"absolute",top:8,right:8 }}>
          <CloseIcon />
        </IconButton>
      </DialogTitle>

      <DialogContent dividers>
        <Stack spacing={3}>
          <Typography variant="subtitle2">
            Model: {report.summary.model} — Total cost: ${report.llm_usage.estimated_cost_usd}
          </Typography>

          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie data={stageData} dataKey="value" cx="50%" cy="50%" outerRadius={80} label>
                {stageData.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>

          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={optData}>
              <XAxis dataKey="name" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="lines" fill="#1976d2" />
            </BarChart>
          </ResponsiveContainer>

          <Stack direction="row" spacing={2}>
            <Button
                variant="outlined"
                onClick={() => doDownload(`/agent/download_final/${jobID}`, finalName)}
              >
                🔽 {finalName}
            </Button>

            <Button
                variant="outlined"
                onClick={() => doDownload(`/agent/download_before/${jobID}`, beforeName)}
              >
                🔽 {beforeName}
             </Button>
          </Stack>
        </Stack>
      </DialogContent>
    </Dialog>
  );
}
