import neostandard from 'neostandard'

// Lint config.
//
// neostandard (a successor to `standard`) is the baseline. We opt into:
//   - `ts: true`   — lint `.ts`/`.d.ts` via typescript-eslint.
//   - `noStyle`    — drop @stylistic formatting rules (this repo has its own
//                    established layout: `function foo(`, multiline trailing
//                    commas, etc.) and keep neostandard's correctness/security
//                    rules.
//   - `env: node`  — Node globals (process, Buffer, ...).
//
// On top of the baseline this project layers a *type-stripping guard*: Node's
// native type stripping (Node >= 24) can only erase *erasable* TypeScript
// syntax. Anything that requires transformation is rejected so the published
// `.ts` source keeps running directly under `node` (no build step).

/** Relative/absolute imports must carry an explicit `.ts` extension (Node ESM
 *  requires extensions and type stripping needs real file specifiers). */
const requireTsExtensions = {
  meta: {
    type: 'problem',
    schema: [],
    messages: {
      noExt: "Relative import '{{ source }}' must include a '.ts' extension (Node ESM + type stripping).",
    },
  },
  create (context) {
    const check = (node) => {
      const source = node.source?.value
      if (typeof source !== 'string') return
      // Only relative/absolute specifiers; leave bare package & `node:` imports alone.
      if (!source.startsWith('.') && !source.startsWith('/')) return
      if (/\.(ts|js|mjs|cjs)$/.test(source)) return
      context.report({ node, messageId: 'noExt', data: { source } })
    }
    return {
      ImportDeclaration: check,
      ExportNamedDeclaration: check,
      ExportAllDeclaration: check,
    }
  },
}

export default [
  ...neostandard({
    ts: true,
    semi: true,
    noStyle: true,
    env: ['node'],
    ignores: ['**/dist/**', '**/coverage/**', '.git/**', 'node_modules/**'],
  }),
  {
    // The type-stripping guard only applies to real `.ts` source, not to
    // ambient `.d.ts` files (which declare types and are never type-stripped).
    files: ['**/*.ts'],
    ignores: ['**/*.d.ts'],
    plugins: {
      local: {
        rules: {
          'require-ts-extension': requireTsExtensions,
        },
      },
    },
    rules: {
      // --- Type-stripping guard (Node native type stripping) ---
      'no-restricted-syntax': [
        'error',
        {
          selector: 'TSEnumDeclaration',
          message: '`enum` is not supported by Node type stripping; use a const object or a union type.',
        },
        {
          selector: 'TSParameterProperty',
          message: 'Parameter properties (`constructor(public x)`) are not supported by Node type stripping.',
        },
        {
          selector: 'TSModuleDeclaration',
          message: '`namespace`/`module` blocks are not supported by Node type stripping.',
        },
        {
          selector: 'TSImportEqualsDeclaration',
          message: '`import x = require(...)` is not supported by Node type stripping.',
        },
      ],
      // --- Require explicit `.ts` extensions on relative imports ---
      'local/require-ts-extension': ['error'],
      // The codebase intentionally uses `_` to mark intentionally-unused
      // bindings (the same pattern tsc's noUnusedLocals tolerates).
      '@typescript-eslint/no-unused-vars': ['error', {
        argsIgnorePattern: '^_',
        varsIgnorePattern: '^_',
        caughtErrorsIgnorePattern: '^_',
        destructuredArrayIgnorePattern: '^_',
        ignoreRestSiblings: true,
      }],
      // Legacy conventions the repo uses deliberately.
      'no-void': 'off',
      'promise/param-names': 'off',
    },
  },
]
