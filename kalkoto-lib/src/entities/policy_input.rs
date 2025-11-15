use crate::entities::policy::Policy;
use crossterm::style::Stylize;
use std::fmt::Display;

#[derive(Debug, Clone)]
pub struct PolicyInput {
    pub valid_policy: Policy,
}

impl Display for PolicyInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "{}",
            "Input Policy correctement initialisé !\n".green().bold()
        )?;
        writeln!(
            f,
            "Politique publique à simuler trouvée dans l'input Policy :",
        )?;
        writeln!(
            f,
            "{}\n",
            self.valid_policy.intitule_long.clone().red().bold()
        )?;
        writeln!(
            f,
            ">>>> Liste ordonnée des composantes de cette politique publique :\n"
        )?;
        let composantes_names = self
            .valid_policy
            .composantes_ordonnees
            .iter()
            .map(|s| format!("- {}", s.name))
            .collect::<Vec<String>>()
            .join("\n");
        writeln!(f, "{}", composantes_names)
    }
}
