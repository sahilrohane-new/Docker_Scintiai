// src/pages/UploadSAS.js
import React, { useState, useEffect } from "react";
import {
  Box, Button, Card, CardContent, Typography, Stack,
  TextField, MenuItem, Dialog, DialogTitle, DialogContent,
  CircularProgress, DialogActions
} from "@mui/material";
import axiosInstance from "../api/axiosInstance";
import { toast } from "react-toastify";
import CountUp from "react-countup";
import AnalysisModal from "../components/AnalysisModal";

/* central lists – keep in sync with backend config ----------------- */
const SOURCES   = ["MS SQL Server","Oracle","Teradata","Apache Hive",
                   "Azure Synapse Analytics","SAP HANA","SAS","PLSQL","Informatica","Datastage","Snowflake","COBOL"];
const DDL_TYPES = ["Tables","Views","Procedures","Functions","General"];
const TARGETS   = ["Databricks","Snowflake","Snowpark","BigQuery","PySpark","DBT","Matillion","Python"];

const SOURCE_FILE_EXT = {
  "SAS": ".sas",
  "Oracle": ".sql",
  "Teradata": ".sql",
  "MS SQL Server": ".sql",
  "Apache Hive": ".sql",
  "Azure Synapse Analytics": ".sql",
  "SAP HANA": ".sql",
  "PLSQL": ".sql",
  "Informatica": ".xml",
  "Datastage": ".xml",
  "Snowflake": ".sql",
  "COBOL": ".cbl,.cob",
};
/*------------------------------------------------------------------ */

/* ▼▼▼ Added: navy hover styling for select menus only ▼▼▼ */
const NAVY = "#001f3f";
const SELECT_HOVER_PROPS = {
  MenuProps: {
    PaperProps: {
      sx: {
        maxHeight: 47 * 4,
        overflowY: "auto",
        "& .MuiMenuItem-root:hover": {
          bgcolor: NAVY,
          color: "#fff",
          fontWeight: 600,
        },
        "& .MuiMenuItem-root.Mui-selected": {
          bgcolor: (theme) => theme.palette.action.selected,
        },
        "& .MuiMenuItem-root.Mui-selected:hover": {
          bgcolor: NAVY,
          color: "#fff",
          fontWeight: 600,
        },
      },
    },
  },
};
/* ▲▲▲ Added: navy hover styling for select menus only ▲▲▲ */

export default function UploadSAS() {
  const [sasFile, setSasFile]           = useState(null);
  const [credId,  setCredId]            = useState("");
  const [creds,   setCreds]             = useState([]);

  const [source,   setSource]           = useState("SAS");
  const [ddlType,  setDdlType]          = useState("General");
  const [target,   setTarget]           = useState("PySpark");

  const [jobId,     setJobId]     = useState("");
  const [busy,      setBusy]      = useState(false);
  const [showPop,   setShowPop]   = useState(false);

  const [logs,      setLogs]      = useState([]);
  const [report,    setReport]    = useState(null);
  const [reportURL, setReportURL] = useState("");
  const [dashOpen,  setDashOpen]  = useState(false);

  const [cost,      setCost]      = useState(null);
  const [costLoad,  setCostLoad]  = useState(false);
  const [downloadPath, setDownloadPath] = useState("");

  /* fetch creds once */
  useEffect(() => {
    axiosInstance.get("/settings/llm").then(r => setCreds(r.data));
  }, []);

  /* polling unchanged */
  useEffect(() => {
    if (!jobId) return;
    const t = setInterval(async () => {
      try {
        const { data } = await axiosInstance.get(`/agent/status/${jobId}`);
        setLogs(data.logs || []);

        if (data.status === "running") return;

        clearInterval(t);
        setBusy(false); setShowPop(false);

        if (data.status === "finished" && data.success) {
          toast.success("Conversion completed");
          setReportURL(data.report || "");
          setDownloadPath(data.download || "");
        } else if (data.status === "stopped") {
          toast.info("⚠️ Conversion force-stopped");
        } else {
          toast.error(data.error || "❌ Job failed");
        }
      } catch {
        clearInterval(t);
        setBusy(false); setShowPop(false);
        toast.error("Polling error");
      }
    }, 2000);
    return () => clearInterval(t);
  }, [jobId]);

  /* helpers -------------------------------------------------------- */
  const buildFormData = () => {
    const fd = new FormData();
    fd.append("file", sasFile);
    fd.append("llm_cred_id", credId);
    fd.append("source", source);
    fd.append("target", target);
    fd.append("ddl_type", ddlType);
    return fd;
  };

  const startJob = async () => {
    if (!sasFile || !credId || !source || !target)
      return toast.error("Select file, credential, source & target");

    setBusy(true); setShowPop(true);
    setLogs([]); setReport(null); setReportURL("");

    try {
      const { data } = await axiosInstance.post("/agent/convert", buildFormData());
      if (data.job_id) setJobId(data.job_id);
      else throw new Error();
    } catch {
      toast.error("Server error"); setBusy(false); setShowPop(false);
    }
  };

  const forceStop = () =>
    jobId && axiosInstance.post(`/agent/force_stop/${jobId}`)
      .then(()=>toast.info("Force-stop sent"))
      .catch(()=>toast.error("Force-stop failed"));

  const estimateCost = async () => {
    if (!sasFile || !credId || !source || !target)
      return toast.error("Select file, credential, source & target");

    setCostLoad(true);
    try {
      const { data } = await axiosInstance.post("/agent/estimate_cost", buildFormData());
      setCost(data);
    } catch { toast.error("Estimation failed"); }
    finally { setCostLoad(false); }
  };

  const openDashboard = async () => {
    if (!reportURL) return toast.warn("Report not ready, try again in a moment");
    if (!report) {
      try { const { data } = await axiosInstance.get(reportURL); setReport(data); }
      catch { return toast.warn("Report not ready, try again shortly"); }
    }
    setDashOpen(true);
  };

  /* UI ------------------------------------------------------------- */
  return (
    <Box sx={{
      p: 4, maxWidth: 1100, mx: "auto",
      mt: { xs: 4, md: 8 },
      bgcolor: "background.paper",
      borderRadius: 3, boxShadow: 3
    }}>
      <Card elevation={4} sx={{ backdropFilter:"blur(12px)" }}>
        <CardContent>
          <Typography variant="h5" mb={3} sx={{ fontWeight:600 }}>
            Convert {source} → {target}
          </Typography>

          <Stack spacing={2}>
            {/* New selectors ------------------------------------------------ */}
            <TextField select label="Source Platform" value={source}
                       onChange={e=>setSource(e.target.value)}
                       SelectProps={SELECT_HOVER_PROPS}>
              {SOURCES.map(s=> <MenuItem key={s} value={s}>{s}</MenuItem>)}
            </TextField>

            <TextField select label="DDL / Script Type" value={ddlType}
                       onChange={e=>setDdlType(e.target.value)}
                       SelectProps={SELECT_HOVER_PROPS}>
              {DDL_TYPES.map(t=> <MenuItem key={t} value={t}>{t}</MenuItem>)}
            </TextField>

            <TextField select label="Target Platform" value={target}
                       onChange={e=>setTarget(e.target.value)}
                       SelectProps={SELECT_HOVER_PROPS}>
              {TARGETS.map(t=> <MenuItem key={t} value={t}>{t}</MenuItem>)}
            </TextField>
            {/* -------------------------------------------------------------- */}

            {/* Credential selector */}
            <TextField
              select fullWidth label="LLM Credential"
              value={credId} onChange={e=>setCredId(e.target.value)}
              SelectProps={SELECT_HOVER_PROPS}
            >
              {creds.map(c=>(
                <MenuItem key={c.id} value={c.id}>
                  {c.name} ({c.provider})
                </MenuItem>
              ))}
            </TextField>

            {/* File chooser */}
            <Button
              component="label"
              variant="outlined"
              sx={{
                color:"#fff",
                borderColor:"rgba(255,255,255,.7)",
                "&:hover":{ borderColor:"#fff", bgcolor:"rgba(255,255,255,.1)" }
              }}
            >
              {sasFile ? sasFile.name : "Choose script file"}
              <input hidden type="file"
                     accept={SOURCE_FILE_EXT[source] || "*"}
                     onChange={e=>setSasFile(e.target.files[0])}/>
            </Button>

            {/* Cost */}
            <Button
              variant="outlined"
              disabled={costLoad}
              onClick={estimateCost}
              sx={{
                color:"#fff",
                borderColor:"rgba(255,255,255,.7)",
                "&:hover":{ borderColor:"#fff", bgcolor:"rgba(255,255,255,.1)" }
              }}
            >
              {costLoad ? "Calculating…" : "Calculate Cost"}
            </Button>

            {cost && (
              <Box sx={{
                p: 2,
                border:"1px dashed rgba(255,255,255,.6)",
                borderRadius: 2,
                bgcolor:"rgba(255,255,255,.06)"
              }}>
                <Typography fontWeight={600}>
                  Estimated tokens / cost
                </Typography>
                <Typography variant="body2">
                  Tokens: {cost.total_tokens} • Cost: $
                  <CountUp end={cost.estimated_cost_usd} decimals={4}/>
                </Typography>
              </Box>
            )}

            {/* Convert */}
            <Button
              variant="contained"
              disabled={busy}
              onClick={startJob}
              sx={{ width:{ xs:"100%", sm:180 } }}
            >
              {busy ? "Running…" : "Convert"}
            </Button>

            {reportURL && (
              <Button variant="outlined" onClick={openDashboard}
                sx={{
                  color:"#fff",
                  borderColor:"rgba(255,255,255,.7)",
                  width:{ xs:"100%", sm:180 },
                  "&:hover":{ borderColor:"#fff", bgcolor:"rgba(255,255,255,.1)" }
                }}>
                📊 View Analysis
              </Button>
            )}
          </Stack>
        </CardContent>
      </Card>

      {/* spinner popup */}
      <Dialog open={showPop} fullWidth maxWidth="xs"
              PaperProps={{ sx:{ textAlign:"center", py:4, backdropFilter:"blur(8px)" } }}>
        <DialogTitle sx={{ fontWeight:600 }}>
          <CircularProgress size={24} sx={{ mr:1 }}/>
          Converting…
        </DialogTitle>
        <DialogContent>
          <Typography variant="body2" sx={{ mb:2 }}>
            Your {source} script is being converted.
          </Typography>
        </DialogContent>
        <DialogActions sx={{ justifyContent:"center" }}>
          <Button
            variant="contained"
            color="error"
            size="small"
            onClick={forceStop}
          >
            ⛔ Force Stop
          </Button>
        </DialogActions>
      </Dialog>

      {/* dashboard */}
      <AnalysisModal
        open={dashOpen}
        onClose={()=>setDashOpen(false)}
        report={report}
        jobID={jobId}
        downloadPath={downloadPath}
      />
    </Box>
  );
}





// // src/pages/UploadSAS.js
// import React, { useState, useEffect } from "react";
// import {
//   Box, Button, Card, CardContent, Typography, Stack,
//   TextField, MenuItem, Dialog, DialogTitle, DialogContent,
//   CircularProgress, DialogActions
// } from "@mui/material";
// import axiosInstance from "../api/axiosInstance";
// import { toast } from "react-toastify";
// import CountUp from "react-countup";
// import AnalysisModal from "../components/AnalysisModal";

// /* central lists – keep in sync with backend config ----------------- */
// const SOURCES   = ["MS SQL Server","Oracle","Teradata","Apache Hive",
//                    "Azure Synapse Analytics","SAP HANA","SAS","PLSQL","Informatica","Datastage"];
// const DDL_TYPES = ["Tables","Views","Procedures","Functions","General"];
// const TARGETS   = ["Databricks","Snowflake","Snowpark","BigQuery","PySpark"];

// const SOURCE_FILE_EXT = {
//   "SAS": ".sas",
//   "Oracle": ".sql",
//   "Teradata": ".sql",
//   "MS SQL Server": ".sql",
//   "Apache Hive": ".sql",
//   "Azure Synapse Analytics": ".sql",
//   "SAP HANA": ".sql",
//   "PLSQL": ".sql",
//   "Informatica": ".xml",
//   "Datastage": ".xml"          // adjust later
// };
// /*------------------------------------------------------------------ */

// export default function UploadSAS() {
//   const [sasFile, setSasFile]           = useState(null);
//   const [credId,  setCredId]            = useState("");
//   const [creds,   setCreds]             = useState([]);

//   const [source,   setSource]           = useState("SAS");
//   const [ddlType,  setDdlType]          = useState("General");
//   const [target,   setTarget]           = useState("PySpark");

//   const [jobId,     setJobId]     = useState("");
//   const [busy,      setBusy]      = useState(false);
//   const [showPop,   setShowPop]   = useState(false);

//   const [logs,      setLogs]      = useState([]);
//   const [report,    setReport]    = useState(null);
//   const [reportURL, setReportURL] = useState("");
//   const [dashOpen,  setDashOpen]  = useState(false);

//   const [cost,      setCost]      = useState(null);
//   const [costLoad,  setCostLoad]  = useState(false);
//   const [downloadPath, setDownloadPath] = useState("");

//   /* fetch creds once */
//   useEffect(() => {
//     axiosInstance.get("/settings/llm").then(r => setCreds(r.data));
//   }, []);

//   /* polling unchanged */
//   useEffect(() => {
//     if (!jobId) return;
//     const t = setInterval(async () => {
//       try {
//         const { data } = await axiosInstance.get(`/agent/status/${jobId}`);
//         setLogs(data.logs || []);

//         if (data.status === "running") return;

//         clearInterval(t);
//         setBusy(false); setShowPop(false);

//         if (data.status === "finished" && data.success) {
//           toast.success("✅ Conversion completed");
//           setReportURL(data.report || "");
//           setDownloadPath(data.download || "");
//         } else if (data.status === "stopped") {
//           toast.info("⚠️ Conversion force-stopped");
//         } else {
//           toast.error(data.error || "❌ Job failed");
//         }
//       } catch {
//         clearInterval(t);
//         setBusy(false); setShowPop(false);
//         toast.error("Polling error");
//       }
//     }, 2000);
//     return () => clearInterval(t);
//   }, [jobId]);

//   /* helpers -------------------------------------------------------- */
//   const buildFormData = () => {
//     const fd = new FormData();
//     fd.append("file", sasFile);
//     fd.append("llm_cred_id", credId);
//     fd.append("source", source);
//     fd.append("target", target);
//     fd.append("ddl_type", ddlType);
//     return fd;
//   };

//   const startJob = async () => {
//     if (!sasFile || !credId || !source || !target)
//       return toast.error("Select file, credential, source & target");

//     setBusy(true); setShowPop(true);
//     setLogs([]); setReport(null); setReportURL("");

//     try {
//       const { data } = await axiosInstance.post("/agent/convert", buildFormData());
//       if (data.job_id) setJobId(data.job_id);
//       else throw new Error();
//     } catch {
//       toast.error("Server error"); setBusy(false); setShowPop(false);
//     }
//   };

//   const forceStop = () =>
//     jobId && axiosInstance.post(`/agent/force_stop/${jobId}`)
//       .then(()=>toast.info("Force-stop sent"))
//       .catch(()=>toast.error("Force-stop failed"));

//   const estimateCost = async () => {
//     if (!sasFile || !credId || !source || !target)
//       return toast.error("Select file, credential, source & target");

//     setCostLoad(true);
//     try {
//       const { data } = await axiosInstance.post("/agent/estimate_cost", buildFormData());
//       setCost(data);
//     } catch { toast.error("Estimation failed"); }
//     finally { setCostLoad(false); }
//   };

//   const openDashboard = async () => {
//     if (!reportURL) return toast.warn("Report not ready, try again in a moment");
//     if (!report) {
//       try { const { data } = await axiosInstance.get(reportURL); setReport(data); }
//       catch { return toast.warn("Report not ready, try again shortly"); }
//     }
//     setDashOpen(true);
//   };

//   /* UI ------------------------------------------------------------- */
//   return (
//     <Box sx={{
//       p: 4, maxWidth: 1100, mx: "auto",
//       mt: { xs: 4, md: 8 },
//       bgcolor: "background.paper",
//       borderRadius: 3, boxShadow: 3
//     }}>
//       <Card elevation={4} sx={{ backdropFilter:"blur(12px)" }}>
//         <CardContent>
//           <Typography variant="h5" mb={3} sx={{ fontWeight:600 }}>
//             Convert {source} → {target}
//           </Typography>

//           <Stack spacing={2}>
//             {/* New selectors ------------------------------------------------ */}
//             <TextField select label="Source Platform" value={source}
//                        onChange={e=>setSource(e.target.value)}>
//               {SOURCES.map(s=> <MenuItem key={s} value={s}>{s}</MenuItem>)}
//             </TextField>

//             <TextField select label="DDL / Script Type" value={ddlType}
//                        onChange={e=>setDdlType(e.target.value)}>
//               {DDL_TYPES.map(t=> <MenuItem key={t} value={t}>{t}</MenuItem>)}
//             </TextField>

//             <TextField select label="Target Platform" value={target}
//                        onChange={e=>setTarget(e.target.value)}>
//               {TARGETS.map(t=> <MenuItem key={t} value={t}>{t}</MenuItem>)}
//             </TextField>
//             {/* -------------------------------------------------------------- */}

//             {/* Credential selector */}
//             <TextField
//               select fullWidth label="LLM Credential"
//               value={credId} onChange={e=>setCredId(e.target.value)}
//             >
//               {creds.map(c=>(
//                 <MenuItem key={c.id} value={c.id}>
//                   {c.name} ({c.provider})
//                 </MenuItem>
//               ))}
//             </TextField>

//             {/* File chooser */}
//             <Button
//               component="label"
//               variant="outlined"
//               sx={{
//                 color:"#fff",
//                 borderColor:"rgba(255,255,255,.7)",
//                 "&:hover":{ borderColor:"#fff", bgcolor:"rgba(255,255,255,.1)" }
//               }}
//             >
//               {sasFile ? sasFile.name : "Choose script file"}
//               <input hidden type="file"
//                      accept={SOURCE_FILE_EXT[source] || "*"}
//                      onChange={e=>setSasFile(e.target.files[0])}/>
//             </Button>

//             {/* Cost */}
//             <Button
//               variant="outlined"
//               disabled={costLoad}
//               onClick={estimateCost}
//               sx={{
//                 color:"#fff",
//                 borderColor:"rgba(255,255,255,.7)",
//                 "&:hover":{ borderColor:"#fff", bgcolor:"rgba(255,255,255,.1)" }
//               }}
//             >
//               {costLoad ? "Calculating…" : "Calculate Cost"}
//             </Button>

//             {cost && (
//               <Box sx={{
//                 p: 2,
//                 border:"1px dashed rgba(255,255,255,.6)",
//                 borderRadius: 2,
//                 bgcolor:"rgba(255,255,255,.06)"
//               }}>
//                 <Typography fontWeight={600}>
//                   Estimated tokens / cost
//                 </Typography>
//                 <Typography variant="body2">
//                   Tokens: {cost.total_tokens} • Cost: $
//                   <CountUp end={cost.estimated_cost_usd} decimals={4}/>
//                 </Typography>
//               </Box>
//             )}

//             {/* Convert */}
//             <Button
//               variant="contained"
//               disabled={busy}
//               onClick={startJob}
//               sx={{ width:{ xs:"100%", sm:180 } }}
//             >
//               {busy ? "Running…" : "Convert"}
//             </Button>

//             {reportURL && (
//               <Button variant="outlined" onClick={openDashboard}
//                 sx={{
//                   color:"#fff",
//                   borderColor:"rgba(255,255,255,.7)",
//                   width:{ xs:"100%", sm:180 },
//                   "&:hover":{ borderColor:"#fff", bgcolor:"rgba(255,255,255,.1)" }
//                 }}>
//                 📊 View Analysis
//               </Button>
//             )}
//           </Stack>
//         </CardContent>
//       </Card>

//       {/* spinner popup */}
//       <Dialog open={showPop} fullWidth maxWidth="xs"
//               PaperProps={{ sx:{ textAlign:"center", py:4, backdropFilter:"blur(8px)" } }}>
//         <DialogTitle sx={{ fontWeight:600 }}>
//           <CircularProgress size={24} sx={{ mr:1 }}/>
//           Converting…
//         </DialogTitle>
//         <DialogContent>
//           <Typography variant="body2" sx={{ mb:2 }}>
//             Your {source} script is being converted.
//           </Typography>
//         </DialogContent>
//         <DialogActions sx={{ justifyContent:"center" }}>
//           <Button
//             variant="contained"
//             color="error"
//             size="small"
//             onClick={forceStop}
//           >
//             ⛔ Force Stop
//           </Button>
//         </DialogActions>
//       </Dialog>

//       {/* dashboard */}
//       <AnalysisModal
//         open={dashOpen}
//         onClose={()=>setDashOpen(false)}
//         report={report}
//         jobID={jobId}
//         downloadPath={downloadPath}
//       />
//     </Box>
//   );
// }
