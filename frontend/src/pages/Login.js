// src/pages/Login.js
import React, { useState, useEffect } from "react";
import {
  Box,
  Button,
  Card,
  CardContent,
  CircularProgress,
  Stack,
  TextField,
  Typography,
  Link,
  Divider,
} from "@mui/material";
import { useNavigate, Link as RouterLink } from "react-router-dom";
import { toast } from "react-toastify";
import { motion } from "framer-motion";
import axiosInstance from "../api/axiosInstance";
import Logo from "../assets/img/ltimindtree_logo.png"; // company logo

export default function Login() {
  const [email, setEmail] = useState("");
  const [pwd, setPwd] = useState("");
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    if (localStorage.getItem("access_token")) navigate("/");
  }, [navigate]);

  const handleLogin = async (e) => {
    e.preventDefault();
    setLoading(true);
    try {
      const { data } = await axiosInstance.post("/auth/login", {
        email,
        password: pwd,
      });
      localStorage.setItem("access_token", data.access_token);
      toast.success("Login successful");
      navigate("/");
    } catch {
      toast.error("Login failed. Please check credentials.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box
      sx={{
        height: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        px: 2,
      }}
    >
      <Card
        component={motion.div}
        elevation={8}
        initial={{ scale: 0.9, opacity: 0 }}
        animate={{ scale: 1, opacity: 1 }}
        transition={{ duration: 0.6 }}
        sx={{
          width: { xs: "100%", sm: 500, md: 760 },
          borderRadius: 4,
          bgcolor: "rgba(255,255,255,0.07)",
          backdropFilter: "blur(16px)",
          border: "1px solid rgba(255,255,255,0.15)",
        }}
      >
        <CardContent
          sx={{
            display: "flex",
            flexDirection: { xs: "column", md: "row" },
            alignItems: "center",
            p: { xs: 3, md: 4 },
            gap: { xs: 3, md: 4 },
          }}
        >
          {/* ── Left side : logo + product ───────────────── */}
          <Box
            sx={{
              flexBasis: { md: "40%" },
              textAlign: "center",
            }}
          >
            <Box
              component="img"
              src={Logo}
              alt="LTIMindtree"
              sx={{ height: 60, mb: 2 }}
            />
            <Typography
              variant="h4"
              sx={{
                fontWeight: 1000,
                fontStyle: "italic",
                color: "secondary.main",
                textShadow: "0 0 8px rgba(0,0,0,.6)",
                mt: -3
              }}
            >
              ScintiAI
            </Typography>
          </Box>

          {/* divider on desktop */}
          <Divider
            orientation="vertical"
            flexItem
            sx={{ display: { xs: "none", md: "block" }, mx: 2 }}
          />

          {/* ── Right side : sign-in form ───────────────── */}
          <Box sx={{ flexBasis: { md: "60%" }, width: "100%" }}>
            <Typography
              variant="h5"
              sx={{
                mb: 2,
                fontWeight: 700,
                color: "secondary.main",
                textShadow: "0 0 8px rgba(0,0,0,.6)",
                textAlign: "center",
              }}
            >
              Sign In
            </Typography>

            <form onSubmit={handleLogin}>
              <Stack spacing={2}>
                <TextField
                  variant="filled"
                  label="Email"
                  fullWidth
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  sx={{
                    "& .MuiFilledInput-root": {
                      bgcolor: "rgba(255,255,255,0.2)",
                      color: "#fff",
                    },
                  }}
                  InputLabelProps={{ style: { color: "#e0e0e0" } }}
                  required
                />
                <TextField
                  variant="filled"
                  type="password"
                  label="Password"
                  fullWidth
                  value={pwd}
                  onChange={(e) => setPwd(e.target.value)}
                  sx={{
                    "& .MuiFilledInput-root": {
                      bgcolor: "rgba(255,255,255,0.2)",
                      color: "#fff",
                    },
                  }}
                  InputLabelProps={{ style: { color: "#e0e0e0" } }}
                  required
                />

                <Button
                  type="submit"
                  variant="contained"
                  fullWidth
                  disabled={loading}
                  endIcon={loading && <CircularProgress size={20} />}
                  sx={{
                    height: 46,
                    borderRadius: 3,
                    background:
                      "linear-gradient(135deg,#004f9e 0%,#0074d9 100%)",
                    "&:hover": {
                      background:
                        "linear-gradient(135deg,#003c7a 0%,#005bb5 100%)",
                    },
                  }}
                >
                  {loading ? "Logging in..." : "Login"}
                </Button>
              </Stack>
            </form>

            <Typography sx={{ mt: 3, color: "#ddd", textAlign: "center" }}>
              Don’t have an account?&nbsp;
              <Link
                component={RouterLink}
                to="/signup"
                underline="hover"
                sx={{ color: "secondary.light" }}
              >
                Signup
              </Link>
            </Typography>
          </Box>
        </CardContent>
      </Card>
    </Box>
  );
}
