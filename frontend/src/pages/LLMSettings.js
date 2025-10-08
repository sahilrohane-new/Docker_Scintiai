// src/pages/LLMSettings.js
import React, { useState, useEffect } from "react";
import axiosInstance from "../api/axiosInstance";
import {
  Box,
  Button,
  Card,
  CardContent,
  CircularProgress,
  MenuItem,
  Select,
  Stack,
  TextField,
  Typography,
  IconButton,
  Skeleton,
} from "@mui/material";
import DeleteIcon from "@mui/icons-material/Delete";
import { toast } from "react-toastify";
import { motion, AnimatePresence } from "framer-motion";

export default function LLMSettings() {
  const [creds, setCreds] = useState([]);
  const [showForm, setShowForm] = useState(false);
  const [provider, setProvider] = useState("");
  const [formData, setFormData] = useState({});
  const [loading, setLoading] = useState(false);
  const [loadingCreds, setLoadingCreds] = useState(false);

  /* fetch stored credentials */
  const loadCreds = async () => {
    setLoadingCreds(true);
    try {
      const { data } = await axiosInstance.get("/settings/llm");
      setCreds(data);
    } catch {
      toast.error("Failed to load credentials");
    } finally {
      setLoadingCreds(false);
    }
  };
  useEffect(() => { loadCreds(); }, []);

  /* save / validate */
  const handleSave = async (replace = false) => {
    setLoading(true);
    try {
      const payload = { ...formData, provider, replace };
      await axiosInstance.post("/settings/llm", payload);
      toast.success("Credential saved & validated");
      setShowForm(false); loadCreds();
    } catch (err) {
      if (err.response?.data?.detail === "limit_per_provider" && !replace) {
        if (window.confirm("Cred exists. Replace it?")) {
          handleSave(true); return;
        }
      }
      toast.error(err.response?.data?.detail ?? "Validation failed");
    } finally { setLoading(false); }
  };

  /* delete */
  const deleteCred = async (id) => {
    if (!window.confirm("Delete this credential?")) return;
    await axiosInstance.delete(`/settings/llm/${id}`);
    toast.info("Credential deleted"); loadCreds();
  };

  /* provider-specific fields */
  const azureInputs  = [
    "OPENAI_API_BASE",
    "OPENAI_API_KEY",
    "OPENAI_API_VERSION",
    "DEPLOYMENT_NAME",
    "MODEL_NAME",
  ];
  const geminiInputs = [
    "GOOGLE_API_KEY",
    "MODEL_NAME", // placeholder, nothing selectable
  ];
  const inputs =
    provider === "azureopenai"
      ? azureInputs
      : provider === "gemini"
      ? geminiInputs
      : [];

  const formVariants = {
    hidden: { opacity: 0, y: -10 },
    show:   { opacity: 1, y: 0 },
    exit:   { opacity: 0, y: 10 },
  };

  /* every required field must be non-empty.
    For Gemini the MODEL_NAME select is disabled, so keep it blank ─
    the button will stay disabled. */
  const required = ["name", ...inputs];
  const allFilled = required.every(
    (f) => formData[f] && formData[f].trim() !== ""
  );

  return (
    <Box
      sx={{
        p: 4,
        maxWidth: 1100,
        mx: "auto",
        mt: { xs: 4, md: 8 },
        bgcolor: "background.paper",
        borderRadius: 3,
        boxShadow: 3,
      }}
    >
      <Typography
        variant="h4"
        mb={3}
        textAlign="center"
        color="secondary.main"
      >
        Manage LLM Credentials
      </Typography>

      <Button
        variant={showForm ? "outlined" : "contained"}
        onClick={() => setShowForm(!showForm)}
        sx={{ mb: 3 }}
      >
        {showForm ? "Close Form" : "Add New Credential"}
      </Button>

      <AnimatePresence>
        {showForm && (
          <motion.div
            initial="hidden"
            animate="show"
            exit="exit"
            variants={formVariants}
            transition={{ duration: 0.3 }}
          >
            <Card elevation={4} sx={{ mb: 4 }}>
              <CardContent>
                <Stack spacing={2}>
                  <TextField
                    label="Friendly Name"
                    name="name"
                    fullWidth
                    onChange={(e) =>
                      setFormData({ ...formData, name: e.target.value })
                    }
                  />

                  <Select
                    value={provider}
                    onChange={(e) => {
                      setProvider(e.target.value);
                      setFormData({}); // reset when switching provider
                    }}
                    fullWidth
                    displayEmpty
                  >
                    <MenuItem value="">Select Provider</MenuItem>
                    <MenuItem value="azureopenai">Azure OpenAI</MenuItem>
                    <MenuItem value="gemini">Gemini (Google)</MenuItem>
                    <MenuItem value="">Anthropic</MenuItem>
                    <MenuItem value="">Meta</MenuItem>
                    <MenuItem value="">Gemini (Google)</MenuItem>
                    <MenuItem value="">DeepSeek</MenuItem>
                    <MenuItem value="">Mistral AI</MenuItem>
                    <MenuItem value="">Snowflake</MenuItem>
                  </Select>

                  {inputs.map((field) =>
                    field === "MODEL_NAME" ? (
                      provider === "azureopenai" ? (
                        /* Azure model dropdown */
                        <Select
                          key={field}
                          name="MODEL_NAME"
                          value={formData.MODEL_NAME || ""}
                          onChange={(e) =>
                            setFormData((p) => ({
                              ...p,
                              MODEL_NAME: e.target.value,
                            }))
                          }
                          fullWidth
                          displayEmpty
                        >
                          <MenuItem value="">Choose Azure Model</MenuItem>
                          <MenuItem value="gpt-4o">gpt-4o</MenuItem>
                          <MenuItem value="gpt-4">gpt-4</MenuItem>
                        </Select>
                      ) : (
                        /* Gemini placeholder dropdown */
                        <Select
                          key={field}
                          name="MODEL_NAME"
                          value={formData.MODEL_NAME || ""}
                          onChange={(e) =>
                            setFormData((p) => ({
                              ...p,
                              MODEL_NAME: e.target.value,
                            }))
                          }
                          fullWidth
                          displayEmpty
                        >
                          <MenuItem value="models/gemini-pro-latest">models/gemini-pro-latest</MenuItem>
                        </Select>
                        </Select>
                      )
                    ) : (
                      /* default text input */
                      <TextField
                        key={field}
                        name={field}
                        label={field}
                        fullWidth
                        onChange={(e) =>
                          setFormData((p) => ({
                            ...p,
                            [field]: e.target.value,
                          }))
                        }
                      />
                    )
                  )}

                  <Button
                    variant="contained"
                    disabled={loading || !provider || !allFilled}
                    onClick={() => handleSave(false)}
                    endIcon={loading && <CircularProgress size={20} />}
                  >
                    {loading ? "Validating..." : "Validate & Save"}
                  </Button>
                </Stack>
              </CardContent>
            </Card>
          </motion.div>
        )}
      </AnimatePresence>

      <Typography variant="h6" mb={2}>
        Saved Credentials
      </Typography>

      {loadingCreds ? (
        <Stack spacing={2}>
          {[...Array(2)].map((_, idx) => (
            <Skeleton
              key={idx}
              variant="rectangular"
              height={80}
              animation="wave"
            />
          ))}
        </Stack>
      ) : creds.length === 0 ? (
        <Typography>No credentials saved yet.</Typography>
      ) : (
        <Stack spacing={2}>
          {creds.map((cred) => (
            <Card
              key={cred.id}
              component={motion.div}
              layout
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
            >
              <CardContent
                sx={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                }}
              >
                <Box>
                  <Typography fontWeight={600}>{cred.name}</Typography>
                  <Typography variant="body2" color="text.secondary">
                    {cred.provider}
                  </Typography>
                </Box>
                <IconButton onClick={() => deleteCred(cred.id)}>
                  <DeleteIcon />
                </IconButton>
              </CardContent>
            </Card>
          ))}
        </Stack>
      )}
    </Box>
  );
}
