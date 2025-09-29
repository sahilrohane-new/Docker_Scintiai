// src/components/Navbar.js
import React, { useState, useEffect } from "react";
import {
  AppBar,
  Toolbar,
  Typography,
  Button,
  Slide,
  useScrollTrigger,
  Menu,
  MenuItem,
  IconButton,
  Avatar,
  Box,
} from "@mui/material";
import { Link as RouterLink, useNavigate, useLocation } from "react-router-dom";
import { motion } from "framer-motion";
import axiosInstance from "../api/axiosInstance";

// 👉 add logo image (place it in src/assets/img/)
import Logo from "../assets/img/ltimindtree_logo.png";

function HideOnScroll({ children }) {
  const trigger = useScrollTrigger();
  return (
    <Slide appear={false} direction="down" in={!trigger}>
      {children}
    </Slide>
  );
}

export default function Navbar({ toggleTheme }) {
  const navigate  = useNavigate();
  const location  = useLocation();

  const [tokenValid, setTokenValid] = useState(null);
  const [email,      setEmail]      = useState("");
  const [anchorEl,   setAnchorEl]   = useState(null);
  const [menuOpen,   setMenuOpen]   = useState(false);

  /* ─── auth check ─────────────────────────────────────────────── */
  useEffect(() => {
    (async () => {
      const token = localStorage.getItem("access_token");
      if (!token) {
        setTokenValid(false);
        navigate("/login");
        return;
      }
      try {
        const res = await axiosInstance.get("/auth/me");
        setEmail(res.data.email);
        setTokenValid(true);
      } catch {
        localStorage.removeItem("access_token");
        setTokenValid(false);
        navigate("/login");
      }
    })();
  }, [navigate]);

  const navLinks = [
    { path: "/",        label: "Home" },
    { path: "/upload",  label: "Upload" },
    { path: "/settings",label: "LLM Config" },
  ];

  /* ─── early return if not authed ─────────────────────────────── */
  if (tokenValid !== true) return null;

  /* ─── render ─────────────────────────────────────────────────── */
  return (
    <HideOnScroll>
      <AppBar
        component={motion.div}
        position="sticky"
        elevation={4}
        initial={{ y: -80 }}
        animate={{ y: 0 }}
        sx={{ bgcolor: "secondary.main", transition: "all .3s" }}
      >
        <Toolbar sx={{ justifyContent: "space-between", py: 0.5 }}>

          {/* ─── brand/logo ───────────────────────────────────── */}
          <Box sx={{ display: "flex", alignItems: "center" }}>
            <Box
              component="img"
              src={Logo}
              alt="LTIMindtree"
              sx={{ height: 33, mr: 1}}
            />
            <Typography
              variant="h6"
              component={RouterLink}
              to="/"
              sx={{
                textDecoration: "none",
                fontWeight: 900,
                fontStyle: 'italic',
                color: "title.main",
                marginTop: 0.20
              }}
            >
              MultiCode Converter - DataMind.AI
            </Typography>
          </Box>

          {/* ─── nav links + avatar ──────────────────────────── */}
          <Box sx={{ display: "flex", gap: 2, alignItems: "center" }}>
            {navLinks.map((link) => (
              <Box key={link.path} sx={{ position: "relative" }}>
                <Button
                  component={RouterLink}
                  to={link.path}
                  color="inherit"
                  sx={{
                    fontWeight: 900,
                    color: "#004f9e",
                    textTransform: "none",
                    "&:hover": { color: "secondary.main",bgcolor: "primary.dark" },
                  }}
                >
                  {link.label}
                </Button>
                {location.pathname === link.path && (
                  <motion.div
                    layoutId="nav-underline"
                    style={{
                      height: 3,
                      width: "100%",
                      background: "#004f9e",
                      borderRadius: 2,
                      position: "absolute",
                      bottom: 0,
                      left: 0,
                    }}
                  />
                )}
              </Box>
            ))}

            <IconButton
              onClick={(e) => {
                setAnchorEl(e.currentTarget);
                setMenuOpen((p) => !p);
              }}
              size="small"
              sx={{
                ml: 2,
                transition: "all .3s",
                "&:hover": {
                  transform: "scale(1.1)",
                  boxShadow: "0 0 8px rgba(255, 255, 255, 0.8)",
                },
              }}
            >
              <Avatar
  sx={{
    width: 40,
    height: 40,
    bgcolor: "rgba(74, 145, 227, 0.73)",   // subtle translucent white
    backdropFilter: "blur(8px)",        // picks up background image
    border: "4px solid rgba(211, 211, 211, 0.9)",
    color: "primary.main",
    fontWeight: 600,
  }}
>
  {email ? email[0].toUpperCase() : "U"}
</Avatar>
            </IconButton>

            <Menu
              anchorEl={anchorEl}
              open={menuOpen}
              onClose={() => setMenuOpen(false)}
              transformOrigin={{ horizontal: "right", vertical: "top" }}
              anchorOrigin={{ horizontal: "right", vertical: "bottom" }}
            >
              <MenuItem disabled>{email}</MenuItem>
              <MenuItem
                onClick={() => {
                  localStorage.removeItem("access_token");
                  setMenuOpen(false);
                  navigate("/login");
                }}
              >
                Logout
              </MenuItem>
            </Menu>
          </Box>
        </Toolbar>
      </AppBar>
    </HideOnScroll>
  );
}
