import { test, expect, Page } from '@playwright/test';

// Selectors target Flutter's web semantics tree (force-enabled at boot in
// main.dart). Each Flutter Semantics(identifier:) surfaces as a
// `flt-semantics-identifier` attribute under the flt-semantics-host.
const byId = (id: string) => `[flt-semantics-identifier="${id}"]`;

// Flutter web maps `Semantics(value:)` to the node's `aria-label`
// attribute (NOT `aria-valuetext`, which the engine never emits). Both the
// ActionBuilder lifecycle node and each note-row carry their token there.
const valueOf = (page: Page, id: string) =>
  page.locator(byId(id)).getAttribute('aria-label');

// Wait for CanvasKit to finish booting and the semantics DOM to exist.
// `flt-semantics-host` is intentionally rendered hidden (off-screen,
// opacity 0), so wait for it ATTACHED rather than visible.
async function waitForApp(page: Page) {
  await page.waitForSelector('flt-semantics-host', {
    timeout: 30_000,
    state: 'attached',
  });
  await page.waitForSelector(byId('login-username'), { timeout: 30_000 });
}

test('submit a note and see it appear in the list', async ({ page }) => {
  await page.goto('/');
  await waitForApp(page);

  // Log in as alice (editor-west) so submit succeeds. The username field
  // already defaults to `alice`; click the login button to authenticate.
  await page.locator(byId('login-button')).click();

  // The submit-note form should mount; wait for its status node.
  await page.waitForSelector(byId('submit-note'), { timeout: 30_000 });

  // Type into the actual <input> the title Semantics wraps. `fill()` sets
  // the value directly — `keyboard.type()` races Flutter's text-field focus
  // setup on web and drops leading characters.
  const title = `e2e note ${Date.now()}`;
  await page.locator(`${byId('submit-note-title')} input`).fill(title);
  await page.locator(byId('submit-note-button')).click();

  // Assert the ActionBuilder lifecycle node reports success.
  await expect
    .poll(async () => valueOf(page, 'submit-note'), { timeout: 15_000 })
    .toBe('success');

  // Assert a note-row carrying our unique title exists. Semantics nodes
  // live under the intentionally-hidden `flt-semantics-host`, so assert the
  // node is ATTACHED (present in the DOM) rather than "visible" — visibility
  // is brittle for an off-screen / opacity-0 host.
  await expect(
    page.locator(byId('note-row'), { hasText: title }),
  ).toBeAttached({ timeout: 15_000 });
});
