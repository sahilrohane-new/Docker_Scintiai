import { createTheme } from "@mui/material/styles";

const navBlue = "#004f9e";
const scarlet = "#ff2400";
const bgGrey   = "#f4f4f4";
const heading = "#004285"

export default createTheme({
  palette: {
    mode: "light",
    primary:   { main: navBlue },
    secondary: { main: bgGrey },
    title: {main: heading},
    background: {
      default: "transparent",
      paper:   "rgba(255,255,255,0.06)"   // translucent glass
    },
    text: { primary: "#eaeaea", secondary: "#bfbfbf" }
  },
  typography: {
    fontFamily: "'Poppins', sans-serif",
    h6: { fontWeight: 700 }
  },
  components: {
    MuiAppBar: {
      styleOverrides: { root: { backgroundColor: navBlue } }
    },
    MuiButton: {
      styleOverrides: {
        root: { borderRadius: 12, fontWeight: 600, textTransform: "none" },
        containedPrimary: {
          background: "linear-gradient(135deg,#004f9e 0%,#0074d9 100%)",
          "&:hover": { background: "linear-gradient(135deg,#003c7a 0%,#005bb5 100%)" }
        }
      }
    },
    MuiMenu: {
      styleOverrides: { paper: { background: "rgba(0,0,0,.8)", color: "#fff" } }
    },
    MuiPaper: {
      styleOverrides: { root: { backdropFilter: "blur(12px)" } }
    }
  }
});
