# Chronicle Queue System - UML Diagrams

This folder contains PlantUML activity diagrams for the Chronicle Queue system's core workflows.

## 📊 Available Diagrams

| Diagram | File | Description |
|---------|------|-------------|
| **Producer Workflow** | `producer_workflow.puml` | Writing operations to Chronicle Queue with WALEntryBatch |
| **Consumer Workflow** | `consumer_workflow.puml` | Reading and processing entries from Chronicle Queue |
| **Corruption Recovery** | `corruption_recovery.puml` | Handling corrupted queue entries during startup and runtime |
| **WALEntryBatch Processing** | `walentrybatch_processing.puml` | gRPC request to WAL write flow |
| **Cache Initialization** | `cache_initialization.puml` | Application startup, cache loading, and WAL replay |
| **Multi-Database Transaction** | `multi_database_transaction.puml` | OP1 pattern with atomic operations across 3 databases |

## 🛠️ Viewing in IntelliJ IDEA

1. **Install PlantUML Plugin**:
   - Go to `Settings → Plugins`
   - Search for "PlantUML integration"
   - Install and restart IntelliJ

2. **View Diagrams**:
   - Open any `.puml` file
   - The diagram will render automatically in the PlantUML tool window
   - Use `Ctrl+Alt+Shift+F` to toggle between code and diagram view

3. **Edit Diagrams**:
   - Edit the `.puml` file directly
   - Preview updates in real-time
   - Use IntelliJ's PlantUML syntax highlighting and auto-completion

## 🎨 Diagram Elements

### Activity Diagram Components Used

- **Start/Stop**: `start` and `stop`
- **Actions**: `:Action text;`
- **Decisions**: `if (condition?) then (yes) ... else (no) ... endif`
- **Loops**: `while (condition?) is (yes) ... endwhile (no)`
- **Partitions**: `partition "Name" { ... }`
- **Notes**: `note left/right: text`
- **Colors**: `#pink:Critical Error;` for highlighting
- **Multi-line**: `:|Line 1\nLine 2|`

### Color Coding

- **Normal flow**: Default colors
- **Errors/Failures**: Pink (`#pink:`)
- **Success**: Green (can add `#lightgreen:`)
- **Warnings**: Yellow (can add `#yellow:`)

## 📝 Modifying Diagrams

### Add Parallel Processing
```plantuml
fork
  :Process A;
fork again
  :Process B;
fork again
  :Process C;
end fork
```

### Add Swim Lanes
```plantuml
|Client|
:Send Request;
|Server|
:Process Request;
|Database|
:Execute Query;
```

### Add More Detail to Decisions
```plantuml
if (Complex Condition?) then (case1)
  :Handle Case 1;
elseif (Another Condition?) then (case2)
  :Handle Case 2;
elseif (Third Condition?) then (case3)
  :Handle Case 3;
else (default)
  :Default Handler;
endif
```

## 🚀 Generating Images

### Command Line
```bash
# Generate PNG for all diagrams
java -jar plantuml.jar -tpng *.puml

# Generate SVG for better quality
java -jar plantuml.jar -tsvg *.puml

# Generate with custom config
java -jar plantuml.jar -config diagram.cfg *.puml
```

### IntelliJ IDEA
- Right-click on `.puml` file → Export Diagram
- Choose format (PNG, SVG, EPS, PDF)
- Select output location

## 📚 PlantUML Resources

- [PlantUML Activity Diagram Guide](https://plantuml.com/activity-diagram-beta)
- [PlantUML Themes](https://plantuml.com/theme)
- [PlantUML Colors](https://plantuml.com/color)
- [PlantUML Online Server](http://www.plantuml.com/plantuml/uml/)

## 🔄 Workflow Overview

1. **Producer** → Writes WALEntryBatch to Chronicle Queue
2. **Consumer** → Reads from Queue and processes to Database
3. **Corruption Recovery** → Handles corrupted entries with skip strategy
4. **Cache Init** → Loads cache on startup with WAL replay
5. **Multi-DB Transaction** → Atomic operations across databases
6. **WALEntryBatch Processing** → gRPC to WAL conversion and validation