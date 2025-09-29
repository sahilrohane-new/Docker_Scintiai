// src/pages/Signup.js
import React, { useState } from "react";
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
import axiosInstance from "../api/axiosInstance";
import { motion } from "framer-motion";
import Logo from "../assets/img/ltimindtree_logo.png"; // company logo

export default function Signup() {
  const [email, setEmail] = useState("");
  const [pwd, setPwd] = useState("");
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  /* create account */
  const handleSignup = async (e) => {
    e.preventDefault();
    setLoading(true);
    try {
      await axiosInstance.post("/auth/signup", { email, password: pwd });
      toast.success("Account created successfully");
      navigate("/login");
    } catch {
      toast.error("Signup failed. Try with another email.");
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
          {/* left : logo + product */}
          <Box sx={{ flexBasis: { md: "40%" }, textAlign: "center" }}>
            <Box component="img" src={Logo} alt="LTIMindtree" sx={{ height: 60, mb: 2 }} />
            <Typography
              variant="h4"
              sx={{
                fontWeight: 800,
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

          {/* right : sign-up form */}
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
              Sign Up
            </Typography>

            <form onSubmit={handleSignup}>
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
                  {loading ? "Creating..." : "Sign Up"}
                </Button>
              </Stack>
            </form>

            <Typography sx={{ mt: 3, color: "#ddd", textAlign: "center" }}>
              Already have an account?&nbsp;
              <Link
                component={RouterLink}
                to="/login"
                underline="hover"
                sx={{ color: "secondary.light" }}
              >
                Login
              </Link>
            </Typography>
          </Box>
        </CardContent>
      </Card>
    </Box>
  );
}
