import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// 64080 은 백엔드(nats-tag-stream). 화면은 64173 에서 띄운다 — Windows 예약 구간 59248~59347 을 피한다.
export default defineConfig({
  plugins: [react()],
  server: { port: 64173, strictPort: true },
  preview: { port: 64174, strictPort: true },
});
