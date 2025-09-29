// src/pages/Home.js
import React, { useEffect, useState } from "react";
import { Box, Typography, CircularProgress } from "@mui/material";
import { motion } from "framer-motion";
import axiosInstance from "../api/axiosInstance";

export default function Home() {
  const [userEmail, setUserEmail] = useState("");
  const [loading, setLoading] = useState(true);

  // fetch user once
  useEffect(() => {
    (async () => {
      try {
        const { data } = await axiosInstance.get("/auth/me");
        setUserEmail(data.email);
      } catch {
        console.error("Failed to fetch user");
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  return (
    <Box
      sx={{
        height: "calc(100vh - 64px)", // full-screen minus navbar
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        textAlign: "center",
        color: "#fff",
        position: "relative",
        overflow: "hidden",
        px: 2,
      }}
    >
      {/* --- layered neural waves (pure SVG, no libs) --- */}
      <Box sx={{ position: "absolute", top: 0, left: 0, width: "100%" }}>
        <svg
          viewBox="0 0 1200 120"
          preserveAspectRatio="none"
          style={{ width: "100%", height: 120 }}
        >
          <path
            d="M0,0 C300,100 900,0 1200,100 L1200 0 L0 0 Z"
            fill="rgba(0,0,0,0.25)"
          />
        </svg>
      </Box>
      <Box sx={{ position: "absolute", top: 35, left: 0, width: "100%" }}>
        <svg
          viewBox="0 0 1200 120"
          preserveAspectRatio="none"
          style={{ width: "100%", height: 110 }}
        >
          <path
            d="M0,0 C300,80 900,10 1200,90 L1200 0 L0 0 Z"
            fill="rgba(0,0,0,0.15)"
          />
        </svg>
      </Box>

      {/* --- animated headline -------------------------------- */}
      <motion.div
        initial={{ y: 30, opacity: 0 }}
        animate={{ y: 0, opacity: 1 }}
        transition={{ duration: 0.9 }}
      >
        <Typography
          variant="h2"
          sx={{
  fontWeight: 800,
  lineHeight: 1.1,
  mb: 2,
  color: "#f5f5f5",
  textShadow: "0 0 10px rgba(255,255,255,.6)",
  animation: "glow 3s ease-in-out infinite alternate",
  "@keyframes glow": {
    "0%":   { textShadow: "0 0 6px  rgba(255,255,255,.3)" },
    "50%":  { textShadow: "0 0 18px rgba(255,255,255,.9)" },
    "100%": { textShadow: "0 0 6px  rgba(255,255,255,.3)" },
  },
}}
        >
          Your AI Migration&nbsp;Copilot
        </Typography>
      </motion.div>

      {/* --- greeting line ------------------------------------ */}
      <motion.div
        initial={{ y: 15, opacity: 0 }}
        animate={{ y: 0, opacity: 1 }}
        transition={{ delay: 0.8 }}
      >
        {loading ? (
          <CircularProgress color="secondary" />
        ) : (
          <Typography variant="h5" sx={{ opacity: 0.9 }}>
            {userEmail ? `Welcome back, ${userEmail}` : "Welcome!"}
          </Typography>
        )}
      </motion.div>
    </Box>
  );
}
