/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::io::{IsTerminal, stderr};

use clap::builder::styling::{AnsiColor, Color, Style};

pub const STYLE_RED: Style = Style::new().fg_color(Some(Color::Ansi(AnsiColor::Red)));
pub const STYLE_GREEN: Style = Style::new().fg_color(Some(Color::Ansi(AnsiColor::Green)));
pub const STYLE_ERROR: Style = Style::new().fg_color(Some(Color::Ansi(AnsiColor::Red))).bold();
pub const STYLE_WARNING: Style = Style::new().fg_color(Some(Color::Ansi(AnsiColor::Yellow))).bold();
pub const STYLE_ARGUMENT: Style = Style::new().fg_color(Some(Color::Ansi(AnsiColor::Yellow)));

#[macro_export]
macro_rules! format_colored {
    ($style:expr, $($arg:tt)*) => {
        format!(
            "{}{}{}",
            $style.render(),
            format!($($arg)*),
            $style.render_reset()
        )
    };
}

#[macro_export]
macro_rules! format_error {
    ($($arg:tt)*) => {
        $crate::format_colored!($crate::printer::STYLE_ERROR, $($arg)*)
    };
}

#[macro_export]
macro_rules! format_warning {
    ($($arg:tt)*) => {
        $crate::format_colored!($crate::printer::STYLE_WARNING, $($arg)*)
    };
}

#[macro_export]
macro_rules! format_argument {
    ($($arg:tt)*) => {
        $crate::format_colored!($crate::printer::STYLE_ARGUMENT, $($arg)*)
    };
}

// For progress lines: a closed terminal or pipe must not stop the work that prints them.
#[macro_export]
macro_rules! try_println {
    ($($arg:tt)*) => {{
        use std::io::Write as _;
        let _ = writeln!(std::io::stdout(), $($arg)*);
    }};
}

#[macro_export]
macro_rules! println_error {
    ($($arg:tt)*) => {
        $crate::printer::eprintln_labelled($crate::printer::STYLE_ERROR, "error:", &format!($($arg)*))
    }
}

#[macro_export]
macro_rules! println_warning {
    ($($arg:tt)*) => {
        $crate::printer::eprintln_labelled($crate::printer::STYLE_WARNING, "warning:", &format!($($arg)*))
    }
}

// Styled only for a terminal, so that a redirected log stays plain; a closed stderr must not stop the work either.
pub fn eprintln_labelled(style: Style, label: &str, message: &str) {
    use std::io::Write as _;
    let style = if stderr().is_terminal() { style } else { Style::new() };
    let _ = writeln!(stderr(), "{}{label}{} {message}", style.render(), style.render_reset());
}
