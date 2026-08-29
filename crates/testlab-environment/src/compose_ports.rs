//! Host port ownership keeps every advertised broker independently reachable.

use std::net::TcpListener;

use crate::ComposeFailure;

pub(super) struct HostPorts {
    ports: Vec<u16>,
    reservations: Vec<TcpListener>,
}

impl HostPorts {
    pub(super) fn reserve(count: usize) -> Result<Self, ComposeFailure> {
        if count == 0 {
            return Err(empty());
        }
        let mut ports = Vec::with_capacity(count);
        let mut reservations = Vec::with_capacity(count);
        for _ in 0..count {
            let reservation = TcpListener::bind(("127.0.0.1", 0)).map_err(|error| {
                ComposeFailure::new(
                    "environment_host_port_unavailable",
                    format!("failed to reserve a loopback port: {error}"),
                )
            })?;
            let port = reservation.local_addr().map_err(|error| {
                ComposeFailure::new(
                    "environment_host_port_unavailable",
                    format!("failed to inspect a loopback reservation: {error}"),
                )
            })?;
            ports.push(port.port());
            reservations.push(reservation);
        }
        Ok(Self {
            ports,
            reservations,
        })
    }

    #[cfg(test)]
    pub(super) fn fixed(first: u16, count: usize) -> Result<Self, ComposeFailure> {
        if first == 0 || count == 0 {
            return Err(empty());
        }
        let ports = (0..count)
            .map(|index| {
                let offset = u16::try_from(index).map_err(|_| overflow())?;
                first.checked_add(offset).ok_or_else(overflow)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            ports,
            reservations: Vec::new(),
        })
    }

    pub(super) fn as_slice(&self) -> &[u16] {
        &self.ports
    }

    pub(super) fn endpoint(&self) -> String {
        self.endpoints().join(",")
    }

    pub(super) fn endpoints(&self) -> Vec<String> {
        self.ports
            .iter()
            .map(|port| format!("127.0.0.1:{port}"))
            .collect::<Vec<_>>()
    }

    pub(super) fn release(&mut self) {
        self.reservations.clear();
    }

    pub(super) fn apply_to(
        &self,
        environment: &mut [(String, String)],
    ) -> Result<(), ComposeFailure> {
        self.apply_named(environment, "KAFKA_HOST_PORT")
    }

    pub(super) fn apply_named(
        &self,
        environment: &mut [(String, String)],
        name: &str,
    ) -> Result<(), ComposeFailure> {
        let first = self.ports.first().copied().ok_or_else(empty)?;
        replace(environment, name, first)?;
        for (index, port) in self.ports.iter().copied().enumerate() {
            replace(environment, &format!("{name}_{}", index + 1), port)?;
        }
        Ok(())
    }

    pub(super) fn append_named(&self, environment: &mut Vec<(String, String)>, name: &str) {
        if let Some(first) = self.ports.first() {
            environment.push((name.to_owned(), first.to_string()));
        }
        environment.extend(
            self.ports
                .iter()
                .enumerate()
                .map(|(index, port)| (format!("{name}_{}", index + 1), port.to_string())),
        );
    }
}

fn replace(
    environment: &mut [(String, String)],
    name: &str,
    port: u16,
) -> Result<(), ComposeFailure> {
    let mut matches = environment.iter_mut().filter(|(key, _)| key == name);
    let Some((_, value)) = matches.next() else {
        return Err(missing(name));
    };
    if matches.next().is_some() {
        return Err(missing(name));
    }
    *value = port.to_string();
    Ok(())
}

fn empty() -> ComposeFailure {
    ComposeFailure::new(
        "environment_host_port_invalid",
        "at least one nonzero host port is required",
    )
}

fn missing(name: &str) -> ComposeFailure {
    ComposeFailure::new(
        "environment_host_port_invalid",
        format!("expected one Compose environment value named {name}"),
    )
}

#[cfg(test)]
fn overflow() -> ComposeFailure {
    ComposeFailure::new(
        "environment_host_port_invalid",
        "fixed host port range overflowed",
    )
}
