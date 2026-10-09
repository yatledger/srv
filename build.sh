#!/bin/bash
# Запускаем сборку проекта
cargo build --release
# Устанавливаем исполняемый бит для бинарника
chmod +x target/release/dagdb
