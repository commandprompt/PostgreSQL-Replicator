/*
 * Headers for handling of 'SET var TO', 'SHOW var' and 'RESET var'
 * statements
 *
 * $Id$
 *
 */
#ifndef VARIABLE_H
#define VARIABLE_H 1

extern bool SetPGVariable(const char *name, const char *value);
extern bool GetPGVariable(const char *name);
extern bool ResetPGVariable(const char *name);

extern void set_default_datestyle(void);

#endif	 /* VARIABLE_H */
