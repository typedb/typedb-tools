/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use rustyline::{
    Cmd, CompletionType, ConditionalEventHandler, Config, Editor, Event, EventHandler, Helper, KeyCode, KeyEvent,
    Modifiers, RepeatCount,
    completion::Completer,
    error::ReadlineError,
    highlight::Highlighter,
    hint::Hinter,
    history::{FileHistory, History},
    validate::{ValidationContext, ValidationResult, Validator},
};

use crate::repl::command::CommandDefinitions;

pub(crate) struct RustylineReader<H: Helper> {
    history_file: PathBuf,
    editor: Editor<H, FileHistory>,

    interrupt_content_empty_ref: Arc<Mutex<bool>>,
}

impl<H: CommandDefinitions> RustylineReader<EditorHelper<H>> {
    pub(crate) fn new(command_helper: H, history_file: PathBuf, multiline: bool) -> Self {
        let config = Config::builder().completion_type(CompletionType::Circular).build();
        let history = FileHistory::new();

        let mut editor = Editor::with_history(config, history).unwrap(); // TODO unwrap

        let helper = EditorHelper { command_definitions: command_helper, multiline };
        editor.set_helper(Some(helper));

        let search_mode = Arc::new(Mutex::new(SearchMode::Normal));
        editor.bind_sequence(
            Event::Any,
            EventHandler::Conditional(Box::new(SearchHistoryModeReset { search_mode: search_mode.clone() })),
        );
        let interrupt_handler = InterruptIfEmptyElseClear::new();
        let interrupt_content_empty_ref = interrupt_handler.content_empty_ref();
        editor.bind_sequence(Event::from(KeyEvent::ctrl('c')), EventHandler::Conditional(Box::new(interrupt_handler)));
        editor.bind_sequence(
            Event::from(KeyEvent(KeyCode::Up, Modifiers::NONE)),
            EventHandler::Conditional(Box::new(SearchHistory { forward: false, search_mode: search_mode.clone() })),
        );
        editor.bind_sequence(
            Event::from(KeyEvent(KeyCode::Down, Modifiers::NONE)),
            EventHandler::Conditional(Box::new(SearchHistory { forward: true, search_mode })),
        );
        let _ = editor.load_history(&history_file);
        Self { editor, history_file, interrupt_content_empty_ref }
    }

    pub(crate) fn readline(&mut self, prompt: &str) -> (rustyline::Result<String>, Option<bool>) {
        match self.editor.readline(prompt) {
            Ok(line) => {
                let _ = self.editor.history_mut().add(line.trim_end());
                let _ = self.editor.append_history(&self.history_file);
                // Rustyline removes the last newline - we'll add back so the input matches what one would get from a  file
                (Ok(format!("{}\n", line)), None)
            }
            Err(ReadlineError::Interrupted) => {
                let interrupt_empty = self.interrupt_content_empty_ref.lock().unwrap().clone();
                (Err(ReadlineError::Interrupted), Some(interrupt_empty))
            }
            Err(err) => (Err(err), None),
        }
    }
}

pub(crate) struct EditorHelper<H> {
    command_definitions: H,
    multiline: bool,
}

impl<H: CommandDefinitions> Helper for EditorHelper<H> {}

impl<H: CommandDefinitions> Completer for EditorHelper<H> {
    type Candidate = H::Candidate;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        self.command_definitions.complete(line, pos, ctx)
    }
}

impl<H: CommandDefinitions> Hinter for EditorHelper<H> {
    type Hint = H::Hint;

    fn hint(&self, line: &str, pos: usize, ctx: &rustyline::Context<'_>) -> Option<Self::Hint> {
        self.command_definitions.hint(line, pos, ctx)
    }
}

impl<H: CommandDefinitions> Highlighter for EditorHelper<H> {
    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        self.command_definitions.highlight_hint(hint)
    }
}

impl<H: CommandDefinitions> Validator for EditorHelper<H> {
    fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        if self.multiline {
            let validator = MultilineValidator { definitions: &self.command_definitions };
            validator.validate(ctx)
        } else {
            Ok(ValidationResult::Valid(None))
        }
    }
}

struct InterruptIfEmptyElseClear {
    interrupted_input_empty: Arc<Mutex<bool>>,
}

impl InterruptIfEmptyElseClear {
    fn new() -> Self {
        Self { interrupted_input_empty: Arc::new(Mutex::new(true)) }
    }

    fn content_empty_ref(&self) -> Arc<Mutex<bool>> {
        self.interrupted_input_empty.clone()
    }
}

impl ConditionalEventHandler for InterruptIfEmptyElseClear {
    fn handle(&self, _evt: &Event, _n: RepeatCount, _positive: bool, ctx: &rustyline::EventContext) -> Option<Cmd> {
        if !ctx.line().is_empty() {
            *self.content_empty_ref().lock().unwrap() = false;
        }
        Some(Cmd::Interrupt)
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum SearchMode {
    Normal,
    InNormalHistory,
    InCompletion,
}

struct SearchHistoryModeReset {
    search_mode: Arc<Mutex<SearchMode>>,
}

impl ConditionalEventHandler for SearchHistoryModeReset {
    fn handle(&self, evt: &Event, _n: RepeatCount, _positive: bool, _ctx: &rustyline::EventContext) -> Option<Cmd> {
        if let Event::KeySeq(keys) = evt {
            if !(keys.contains(&KeyEvent(KeyCode::Up, Modifiers::NONE))
                || keys.contains(&KeyEvent(KeyCode::Down, Modifiers::NONE)))
            {
                *self.search_mode.lock().unwrap() = SearchMode::Normal;
            }
        }
        None
    }
}

struct SearchHistory {
    forward: bool,
    search_mode: Arc<Mutex<SearchMode>>,
}

impl ConditionalEventHandler for SearchHistory {
    fn handle(&self, _evt: &Event, _n: RepeatCount, _positive: bool, ctx: &rustyline::EventContext) -> Option<Cmd> {
        if ctx.line().is_empty() {
            *self.search_mode.lock().unwrap() = SearchMode::InNormalHistory;
            if self.forward { Some(Cmd::NextHistory) } else { Some(Cmd::PreviousHistory) }
        } else {
            let mode = *self.search_mode.lock().unwrap();
            match mode {
                SearchMode::Normal => {
                    *self.search_mode.lock().unwrap() = SearchMode::InCompletion;
                }
                SearchMode::InNormalHistory => {
                    // stay cycling through normal history
                }
                SearchMode::InCompletion => {
                    // stay in completion mode
                }
            }
            match *self.search_mode.lock().unwrap() {
                SearchMode::Normal => unreachable!("Must have picked a mode by the time we search searching."),
                SearchMode::InNormalHistory => {
                    if self.forward {
                        Some(Cmd::LineDownOrNextHistory(1))
                    } else {
                        Some(Cmd::LineUpOrPreviousHistory(1))
                    }
                }
                SearchMode::InCompletion => {
                    if self.forward {
                        Some(Cmd::HistorySearchForward)
                    } else {
                        Some(Cmd::HistorySearchBackward)
                    }
                }
            }
        }
    }
}

struct MultilineValidator<'a, D: CommandDefinitions> {
    definitions: &'a D,
}

impl<'a, D: CommandDefinitions> Validator for MultilineValidator<'a, D> {
    fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        /*
        RustLine trims off the last newline by the time we get here. As a result, when we have input:
        match  [newline]

        We only see 'match' without the newline. However, we can assume there is a newline at the end by the time we get here.
        As a result, when we want to validate a double-newline entry the user, we just have to check if the last character is newline!
        */
        if self.definitions.is_complete_command(&format!("{}\n", ctx.input())) {
            Ok(ValidationResult::Valid(None))
        } else {
            Ok(ValidationResult::Incomplete)
        }
    }
}

pub(crate) struct LineReaderHidden {}

impl LineReaderHidden {
    pub(crate) fn new() -> Self {
        Self {}
    }

    pub(crate) fn readline(&self, prompt: &str) -> String {
        rpassword::prompt_password(prompt).unwrap()
    }
}
