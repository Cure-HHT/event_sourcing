// Shared, app-agnostic Playwright helpers for driving a Flutter web app
// through its semantics tree (CUR-1307).
//
// Every interactive target the app annotates with `AutomationTarget`
// (see lib/client/automation.dart) surfaces as a
// `flt-semantics-identifier` attribute under the `flt-semantics-host`.
// These helpers encapsulate the web-semantics quirks so specs read as
// plain intent.
import { expect, Page } from '@playwright/test';

/// CSS selector for a Flutter `Semantics(identifier:)` node.
export const byId = (id: string): string =>
  `[flt-semantics-identifier="${id}"]`;

/// Wait for CanvasKit to finish booting and the semantics DOM to exist.
/// `flt-semantics-host` is rendered intentionally hidden (off-screen,
/// opacity 0), so wait for it ATTACHED rather than visible. Callers wait
/// for their own first identifier afterwards — this only gates on the host.
export async function waitForFlutter(page: Page): Promise<void> {
  await page.waitForSelector('flt-semantics-host', {
    timeout: 60_000,
    state: 'attached',
  });
}

/// Read a node's machine-readable value. The carrier varies by node role
/// on web: a `Semantics(value:)` lands on `aria-label` for most nodes, but
/// some role-bearing nodes expose it only as text content. Prefer
/// `aria-label`; fall back to trimmed `textContent`.
export async function readSemanticValue(
  page: Page,
  id: string,
): Promise<string | null> {
  const node = page.locator(byId(id));
  const ariaLabel = await node.getAttribute('aria-label');
  if (ariaLabel !== null) return ariaLabel;
  const text = await node.textContent();
  return text === null ? null : text.trim();
}

/// Fill a text field annotated via `AutomationTarget(field: true)`.
///
/// Targets `input:not([disabled])`: the wrapper's `textField: true` flag
/// spawns a PHANTOM DISABLED `<input>` on the wrapper node alongside the
/// real editable one (nested in a child semantics node), so a bare `input`
/// selector is ambiguous and the disabled one would never accept input
/// (waitFor/fill would time out).
///
/// Three steps, each load-bearing on Flutter web:
///
///  1. Click the field node FIRST. The enabled semantic input only becomes
///     Flutter's live editing element once the field is focused; filling an
///     un-focused semantic input writes a value that Flutter then wipes on
///     its next semantics rebuild (the controller never sees it), which
///     makes a bare fill flaky — the submit reads an empty controller and
///     no-ops.
///  2. `.fill()` rather than `keyboard.type()`: typing races Flutter's
///     text-field setup and drops leading characters; `.fill()` sets the
///     value directly on the now-focused input.
///  3. Wait for the value to land (`toHaveValue`) before returning, so the
///     caller's next action (e.g. a submit click) doesn't read the bound
///     `TextEditingController` before Flutter has committed the edit.
export async function fillField(
  page: Page,
  id: string,
  value: string,
): Promise<void> {
  const input = page.locator(`${byId(id)} input:not([disabled])`);
  await page.locator(byId(id)).click();
  // The click must land Flutter's editing focus on this input before we
  // fill — otherwise the fill writes to an input Flutter promptly wipes.
  await expect(input).toBeFocused();
  await input.fill(value);
  await expect(input).toHaveValue(value);
}
