/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

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

#[macro_export]
macro_rules! println_error {
    ($($arg:tt)*) => {
        eprintln!(
            "{} {}",
            $crate::format_error!("error:"),
            format!($($arg)*)
        );
    }
}

#[macro_export]
macro_rules! println_warning {
    ($($arg:tt)*) => {
        eprintln!(
            "{} {}",
            $crate::format_warning!("warning:"),
            format!($($arg)*)
        );
    }
}
