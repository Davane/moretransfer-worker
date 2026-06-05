---
name: coding-best-practices
description: "Core coding standards for writing clean, safe, maintainable, and consistent code"
---

# Coding Best Practices

Apply these standards whenever writing, modifying, or reviewing code.

## General Principles

- Prefer clear, simple, maintainable code over clever code.
- Keep changes focused and avoid unrelated refactors.
- Follow existing project conventions unless they conflict with these rules.
- Avoid new dependencies unless clearly justified.
- Make behavior explicit and easy to understand.

## Formatting

- Always use curly braces for `if`, `else`, `for`, `while`, and similar blocks, even for one-line statements.

```ts
if (isReady) {
  startUpload();
}
```

- Use the project formatter and keep formatting consistent.
- Avoid deeply nested code; use early returns when they improve readability.

## Naming

- Use descriptive names that explain intent.
- Avoid vague names like `data`, `item`, `thing`, `temp`, or `result` unless the scope is very small.
- Use clear boolean names like `isEnabled`, `hasExpired`, `canRetry`, and `shouldNotify`.
- Use action-based function names like `createTransfer`, `sendEmail`, or `validateAccess`.

## Functions

- Keep functions focused on one clear responsibility.
- Avoid mixing validation, database access, business logic, and side effects in one large function.
- Prefer explicit arguments over hidden global state.
- Validate inputs at system boundaries such as API routes, queue handlers, webhooks, and public functions.

## Comments and Docstrings

- Add docstring comments to complex functions, classes, types, interfaces, and modules.
- Use docstrings to explain what the code does, why it exists, caveats, assumptions, side effects, failure behavior, and idempotency rules.

```ts
/**
 * Sends a transfer expiry notification.
 *
 * Safe to retry when called with a deterministic idempotency key.
 * Does not decide eligibility or mutate the transfer expiry date.
 */
async function sendTransferExpiryNotification(params: SendExpiryNotificationParams) {
  // ...
}
```

- Avoid comments that simply repeat the code.
- Use comments to explain why something exists, not just what it does.

## Types

- Prefer explicit types for exported functions, shared utilities, API responses, and domain objects.

- Avoid `any`; use `unknown` and narrow safely when needed.
- Use union types or enums for known statuses and events.
- Prefer string union types for domain statuses, lifecycle states, event names, and other fixed string values.
- If runtime lookup is needed, prefer an `as const` object plus a derived union type.
- Make invalid states hard to represent where practical.

## Error Handling

- Handle expected errors explicitly.
- Do not silently swallow errors.
- Include useful debugging context in logs without exposing secrets or sensitive data.
- Return safe, understandable user-facing errors.
- Preserve the original error when wrapping failures.
