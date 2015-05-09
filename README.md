The HAskelL File System
===

Intended to be used with HaLVM.

## Description
A library implementing a file system suitable for use in HaLVMs.  Provides useful abstractions over the underlying block layer.

## Get started
The main `cabal` package is the library compatible with HaLVM, called `halfs`. you may want to `halvm-cabal build` and `halvm-cabal install` it. Then you can find some examples in HaLVM's examples (`examples/HighLevel/Halfs`). Happy hacking!

## Use `halfs-test` with Unix and Fuse
First, install HFuse package (with `devel` version) into your host OS with any package manager, for example:

```
yum install fuse fuse-devel
```
Go to `test` first.

Then go to `deps/hfuse`, `cabal install` the `hfuse` dependency. This package has an [upstream](https://github.com/m15k/hfuse).

Finally, go back to `test` again, `cabal configure`, then `cabal build`.

You can test it with `test/dist/build/halfs-tests` binary. There is also a CLI tool interfacing the HFuse as well (`test/dist/build/halfs/halfs`), but this tool seems a little incomplete.