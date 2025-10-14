import axios from "axios";

const axiosInstance = axios.create({
  baseURL: "https://backend-service-68306044937.us-central1.run.app", // Adjust if your FastAPI is at a different URL
});

// Attach token to headers if present
axiosInstance.interceptors.request.use((config) => {
  const token = localStorage.getItem("access_token");
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

export default axiosInstance;
