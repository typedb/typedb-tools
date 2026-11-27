## Distribution

Download from TypeDB Package Repository: https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typeql-check+version:3.16.0-rc0


## New Features

## Bugs Fixed

## Code Refactors
- **Simplify workspace referencing**
  
  Each TypeDB dependency's commit or tag is now written once, in its `git_override` in `MODULE.bazel`. 
  
  Before, refs were duplicated in `workspace_refs`' commit and tag dicts, with commented-out alternatives for switching between the two. 
  
  
  
## Other Improvements
- **Update TypeQL, TypeDB Driver and TypeDB Protocol for the vector value type**
  Build against the TypeQL, TypeDB Driver and TypeDB Protocol versions that introduce the vector value type.
  
  
    
