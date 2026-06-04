import { defineConfig, devices } from '@playwright/test';

// The dart demo server (port 8080) and the `flutter build web` step are
// orchestrated by scripts/run-e2e.sh. Playwright only serves the built
// bundle and runs the specs against Chromium.
export default defineConfig({
  testDir: './tests',
  timeout: 60_000,
  expect: { timeout: 15_000 },
  fullyParallel: false,
  retries: 0,
  reporter: [['list']],
  use: {
    baseURL: 'http://localhost:8000',
    // retries is 0, so 'on-first-retry' would never fire; capture a trace
    // whenever a test fails instead.
    trace: 'retain-on-failure',
  },
  projects: [
    { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
  ],
  webServer: {
    // `serve` is pinned as a devDependency (see package.json) so this
    // resolves the locked local binary rather than fetching latest.
    command: 'npx serve ../build/web -l 8000 --no-clipboard',
    url: 'http://localhost:8000',
    reuseExistingServer: !process.env.CI,
    timeout: 60_000,
  },
});
