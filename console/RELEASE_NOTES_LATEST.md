## Distribution

Download from TypeDB Package Repository: https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-console+version:3.12.0


## New Features


## Bugs Fixed


## Code Refactors


## Other Improvements
- **Fix Windows deployment jobs**

- **Fix CI**

- **Add TypeQL Check binary**
  
  Introduces `typeql-check`, a small standalone CLI that validates whether a string is a syntactically valid TypeQL query.
  
  Two ways to pass a query:
  
  ```bash
  # As an argument
  typeql-check "match $x isa person;"
  
  # From stdin
  cat query.tql | typeql-check
  echo "match $x isa person;" | typeql-check
  ```
  
  
- **Update dependencies to TypeDB repos**
  
  Move lingering references to `krishnangovindraj` repos to point to `typedb` repos with updated commits
  
  
- **Trigger CI**

- **CSV Loader for TypeDB**
  
  - Add the new TypeDB Loader CLI tool, for loading CSV data into TypeDB
    - The loader uses the upcoming 'inputs' stage
  - Split the repo into two main subfolders - `console` and `loader`, containing the respective CLI tools
  - Migrate CI processes to use GitHub Actions
  
  
- **Set bazel cache size and age limit**

    
