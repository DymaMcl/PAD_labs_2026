# PAD 2026 — Programarea Aplicațiilor Distribuite

Autor: **Dumitru Micleușanu**

Fiecare laborator este dezvoltat pe un branch separat:

| Branch        | Laborator                                   |
|---------------|---------------------------------------------|
| [`laborator_1`](https://github.com/DymaMcl/PAD_labs_2026/tree/laborator_1/laborator_1) | Comunicare fiabilă și mesagerie (producer → broker → consumer) |

## Interfața web (Rust)

Pe branch-urile `laborator_1` și `laborator_2`, folderul [`interfata_rust`](interfata_rust) conține un panou de control scris în Rust, pentru rularea și prezentarea laboratoarelor din browser:

```
cd interfata_rust
cargo run --release        # apoi deschide http://localhost:8080/
```
