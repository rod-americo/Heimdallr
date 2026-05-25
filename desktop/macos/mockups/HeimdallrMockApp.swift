import Cocoa

final class AppDelegate: NSObject, NSApplicationDelegate {
    private var statusItem: NSStatusItem?

    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApp.setActivationPolicy(.regular)

        if let appIcon = NSImage(named: "Heimdallr") {
            NSApp.applicationIconImage = appIcon
        }

        let item = NSStatusBar.system.statusItem(withLength: NSStatusItem.squareLength)
        if let button = item.button {
            button.image = makeTemplateStatusImage()
            button.imagePosition = .imageOnly
            button.imageScaling = .scaleProportionallyDown
        }

        let menu = NSMenu()
        menu.addItem(NSMenuItem(title: "Heimdallr Mock", action: nil, keyEquivalent: ""))
        menu.addItem(NSMenuItem.separator())
        menu.addItem(NSMenuItem(
            title: "Quit Heimdallr Mock",
            action: #selector(NSApplication.terminate(_:)),
            keyEquivalent: "q"
        ))
        item.menu = menu
        statusItem = item
    }

    private func makeTemplateStatusImage() -> NSImage {
        let image = NSImage(named: "HeimdallrMenuTemplate") ?? NSImage()
        image.size = NSSize(width: 18, height: 18)
        image.isTemplate = true
        return image
    }
}

let app = NSApplication.shared
let delegate = AppDelegate()
app.delegate = delegate
app.run()
