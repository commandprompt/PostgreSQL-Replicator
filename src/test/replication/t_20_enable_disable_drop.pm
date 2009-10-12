package t_20_enable_disable_drop;

use MammothTest;
use BaseTest;
use strict;

our @ISA = qw(BaseTest);

sub run
{
	my $self = shift;

	$self->SUPER::run;

	eval {
		my ($master, $slave, $mcp);

		$self->clean_output;

		$master = MammothTest::init_new_pg_node($self, {
				name => 'master',
				init => 1,
				additional_config => 'master.conf',
				prepare_script => 'prepare.sql'
			});

		$slave = MammothTest::init_new_pg_node($self, {
				name => "slave0",
				init => 1,
				additional_config => 'slave0.conf',
				prepare_script => 'prepare.sql'
			});

		$mcp = MammothTest::run_new_mcp($self);

		$slave->start(1);
		$master->start(1);
		$self->execute({
				file => "enable-repl.sql",
				servers => [$master]
			});
		$self->execute({
				servers => [$master],
				file => "disable-repl.sql"
			});
		$self->execute({
				servers => [$slave],
				file => "droptable.sql"
			});
	};

	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}
