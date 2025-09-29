import React, { useMemo, useState } from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import {
  CssBaseline,
  ThemeProvider,
  GlobalStyles
} from "@mui/material";
import { ToastContainer } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";
import refTheme from "./theme/refTheme";

const Root = () => {
  const [mode] = useState("light");          // keeps future dark-mode hook
  const theme  = useMemo(() => refTheme, [mode]);

  return (
    <ThemeProvider theme={theme}>
      {/* hero background without extra css file */}
      <GlobalStyles styles={{
  'html, body, #root': { height: '100%' },
  body: {
    margin: 0,
    /* ✅ absolute path that always works from CRA dev-server and build */
    background: `linear-gradient(rgba(57, 57, 57, 0.45),rgba(0,0,0,.45)), 
                 url("${process.env.PUBLIC_URL}/assets/img/bg3.png") 
                 center/cover no-repeat fixed !important`,
    /* Option B – blend (comment out A if you use this) */
    /* background: `url("${process.env.PUBLIC_URL}/assets/img/bg.jpg") center/cover no-repeat fixed !important`, */
    /* backgroundBlendMode: 'multiply', */
  },
}} />
      <CssBaseline />
      <App />
      <ToastContainer position="bottom-right" />
    </ThemeProvider>
  );
};

ReactDOM.createRoot(document.getElementById("root")).render(<Root />);
