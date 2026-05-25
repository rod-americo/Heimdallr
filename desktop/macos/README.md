# macOS App

This directory is reserved for the future Swift menu bar app.

## Intended Responsibilities

- show current daemon and worker state;
- guide OsiriX/Horos inbound C-STORE setup;
- collect OsiriX/Horos egress destination settings;
- select segmentation profile and conservative device policy;
- start, stop, and restart services through `heimdallrd`;
- show log and validation summaries with actionable status.

## First UI Scope

The first proof of concept should include:

- menu bar status indicator;
- setup view for inbound Heimdallr DICOM settings;
- setup view for outbound OsiriX/Horos listener settings;
- segmentation profile picker;
- service controls;
- diagnostic actions for ports and DICOM connectivity;
- link to logs.

The app should treat the Go daemon as its backend. It should not start Python
workers directly once the daemon exists.

## Menu Bar Icon Contract

Use the `MenuBarIcon.imageset` assets generated from
`docs/branding/vectors/heimdallr_icon_variant_01_black.svg`. The status item
should use `NSStatusItem.squareLength`, load the template image by asset name,
set a logical size of `18 x 18`, and let AppKit render it for light, dark, and
selected menu bar states.

Reference implementation:

```swift
let item = NSStatusBar.system.statusItem(withLength: NSStatusItem.squareLength)
if let button = item.button {
    let image = NSImage(named: "HeimdallrMenuTemplate")
    image?.size = NSSize(width: 18, height: 18)
    image?.isTemplate = true
    button.image = image
    button.imagePosition = .imageOnly
    button.imageScaling = .scaleProportionallyDown
}
```

Do not redraw the icon with custom Swift/CoreGraphics paths. The SVG source is
the geometry contract, and the accepted raster baseline is documented in
`desktop/macos/assets/README.md`.
