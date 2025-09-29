import React from "react";
import { BrowserRouter, Routes, Route, Navigate, useLocation } from "react-router-dom";
import Navbar from "./components/Navbar";
import Login from "./pages/Login";
import Signup from "./pages/Signup";
import Home from "./pages/Home";
import UploadSAS from "./pages/UploadSAS";
import LLMSettings from "./pages/LLMSettings";
import ManualFix from "./pages/ManualFix"; // ✅ new import
import { AnimatePresence, motion } from "framer-motion";

function ProtectedRoute({ children }) {
  const token = localStorage.getItem("access_token");
  if (!token) {
    return <Navigate to="/login" replace />;
  }
  return children;
}

function PublicRoute({ children }) {
  const token = localStorage.getItem("access_token");
  if (token) {
    return <Navigate to="/" replace />;
  }
  return children;
}

function AnimatedRoutes() {
  const location = useLocation();

  return (
    <AnimatePresence mode="wait">
      <Routes location={location} key={location.pathname}>
        <Route
          path="/login"
          element={
            <PublicRoute>
              <PageWrapper><Login /></PageWrapper>
            </PublicRoute>
          }
        />
        <Route
          path="/signup"
          element={
            <PublicRoute>
              <PageWrapper><Signup /></PageWrapper>
            </PublicRoute>
          }
        />
        <Route
          path="/"
          element={
            <ProtectedRoute>
              <PageWrapper><Home /></PageWrapper>
            </ProtectedRoute>
          }
        />
        <Route
          path="/upload"
          element={
            <ProtectedRoute>
              <PageWrapper><UploadSAS /></PageWrapper>
            </ProtectedRoute>
          }
        />
        <Route
          path="/settings"
          element={
            <ProtectedRoute>
              <PageWrapper><LLMSettings /></PageWrapper>
            </ProtectedRoute>
          }
        />
        <Route
          path="/manual-fix"
          element={
            <ProtectedRoute>
              <PageWrapper><ManualFix /></PageWrapper>
            </ProtectedRoute>
          }
        />

        {/* fallback */}
        <Route path="*" element={<Navigate to="/" />} />
      </Routes>
    </AnimatePresence>
  );
}

function PageWrapper({ children }) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -10 }}
      transition={{ duration: 0.4 }}
    >
      {children}
    </motion.div>
  );
}

export default function App({ toggleTheme }) {
  const token = localStorage.getItem("access_token");

  return (
    <BrowserRouter>
      {token && <Navbar toggleTheme={toggleTheme} />}
      <AnimatedRoutes />
    </BrowserRouter>
  );
}
