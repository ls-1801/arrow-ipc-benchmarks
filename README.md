# Building
## Dependencies
Mostly based on the [NebulaStream dependencies](https://github.com/nebulastream/nebulastream-dependencies/releases/tag/v30)
Make sure to use the `-DCMAKE_TOOLCHAIN_FILE=path/to/vcpkg.cmake` and `-DVCPKG_TARGET_TRIPLET=x64-linux-nes`

Additional dependencies:
- ArrowFight:
    - you may have to overwrite the ArrowFlight Directory `-DArrowFlight_DIR=path/to/nes-deps/installed/x64-linux-nes/share/arrow`
- [Argsparse](https://github.com/p-ranav/argparse) for CLI Parsing
- Boost Interprocess 
- Shared Memory Benchmark using [Lightning](https://github.com/danyangz/lightning)
    - Patched version which fixes missing headers [patch](https://github.com/ls-1801/lightning)
    - Build the cmake project and copy the inc and lib folder and set `-DLightning_DIR`




