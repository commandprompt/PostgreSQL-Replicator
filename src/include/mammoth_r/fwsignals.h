/*
 * 		fwsignals.h
 *			Definitions and declarations for forwarder signals.
 *
 * $Id: fwsignals.h 2091 2009-04-06 17:34:04Z alexk $
 */

#ifndef FWSIGNALS_H
#define FWSIGNALS_H

/* forwarder signal types */
typedef enum
{
	FORWARDER_SIGNAL_PROMOTION,
	FORWARDER_SIGNAL_CHECK_ENCODING,
	NUM_FORWARDER_SIGNALS /* must be the last value of enum */
} ForwarderSignalReason;

/* prototypes from fwsignals.c */
void FWSignalsInit(void);
void SendForwarderChildSignal(int nodeno, ForwarderSignalReason reason);
bool CheckForwarderChildSignal(ForwarderSignalReason reason);
void ResetForwarderChildSignals(void);

#endif /* FWSIGNALS_H */
