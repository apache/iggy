export default {
  extends: ['@commitlint/config-conventional'],
  rules: {
    "scope-case": [2, "always", ["lower-case", "upper-case", "camel-case"]],
    "footer-max-line-length": [2 ,"always", [256]]
  }
};
